require 'nats/io/parser'
require 'nats/io/version'
require 'thread'
require 'socket'
require 'json'
require 'monitor'
require 'uri'
require 'securerandom'

begin
  require "openssl"
rescue LoadError
end

module NATS
  module IO

    DEFAULT_PORT = 4222
    DEFAULT_URI = "nats://localhost:#{DEFAULT_PORT}".freeze

    MAX_RECONNECT_ATTEMPTS = 10
    RECONNECT_TIME_WAIT = 2

    # Maximum accumulated pending commands bytesize before forcing a flush.
    MAX_PENDING_SIZE = 32768

    # Maximum number of flush kicks that can be queued up before we block.
    MAX_FLUSH_KICK_SIZE = 1024

    # Maximum number of bytes which we will be gathering on a single read.
    # TODO: Make dynamic?
    MAX_SOCKET_READ_BYTES = 32768

    # Ping intervals
    DEFAULT_PING_INTERVAL = 120
    DEFAULT_PING_MAX = 2

    # Default IO timeouts
    DEFAULT_CONNECT_TIMEOUT = 2
    DEFAULT_READ_WRITE_TIMEOUT = 2

    CR_LF = ("\r\n".freeze)
    CR_LF_SIZE = (CR_LF.bytesize)

    PING_REQUEST  = ("PING#{CR_LF}".freeze)
    PONG_RESPONSE = ("PONG#{CR_LF}".freeze)

    SUB_OP = ('SUB'.freeze)
    EMPTY_MSG = (''.freeze)

    # Connection States
    DISCONNECTED = 0
    CONNECTED    = 1
    CLOSED       = 2
    RECONNECTING = 3
    CONNECTING   = 4

    class Error < StandardError; end

    # When the NATS server sends us an 'ERR' message.
    class ServerError < Error; end

    # When we detect error on the client side.
    class ClientError < Error; end

    # When we cannot connect to the server (either initially or after a reconnect).
    class ConnectError < Error; end

    # When we cannot connect to the server because authorization failed.
    class AuthError < ConnectError; end

    # When we cannot connect since there are no servers available.
    class NoServersError < ConnectError; end

    # When the connection exhausts max number of pending pings replies.
    class StaleConnectionError < Error; end

    # When we do not get a result within a specified time.
    class Timeout < Error; end

    # When there is an i/o timeout with the socket.
    class SocketTimeoutError < Error; end

    # When we use an invalid subject.
    class BadSubject < Error; end

    class Client
      include MonitorMixin

      attr_reader :status, :server_info, :server_pool, :options, :connected_server, :stats, :uri

      def initialize
        super # required to initialize monitor
        @options = nil

        # Read/Write IO
        @io = nil

        # Queues for coalescing writes of commands we need to send to server.
        @flush_queue = nil
        @pending_queue = nil

        # Parser with state
        @parser = NATS::Protocol::Parser.new(self)

        # Threads for both reading and flushing command
        @flusher_thread = nil
        @read_loop_thread = nil
        @ping_interval_thread = nil

        # Info that we get from the server
        @server_info = { }

        # URI from server to which we are currently connected
        @uri = nil
        @server_pool = []

        @status = DISCONNECTED

        # Subscriptions
        @subs = { }
        @ssid = 0

        # Ping interval
        @pings_outstanding = 0
        @pongs_received = 0
        @pongs = []
        @pongs.extend(MonitorMixin)

        # Accounting
        @pending_size = 0
        @stats = {
          in_msgs: 0,
          out_msgs: 0,
          in_bytes: 0,
          out_bytes: 0,
          reconnects: 0
        }

        # Sticky error
        @last_err = nil

        # Async callbacks, no ops by default.
        @err_cb = proc { }
        @close_cb = proc { }
        @disconnect_cb = proc { }
        @reconnect_cb = proc { }

        # Secure TLS options
        @tls = nil
      end

      # Establishes connection to NATS
      def connect(opts={})
        opts[:verbose] = false if opts[:verbose].nil?
        opts[:pedantic] = false if opts[:pedantic].nil?
        opts[:reconnect] = true if opts[:reconnect].nil?
        opts[:reconnect_time_wait] = RECONNECT_TIME_WAIT if opts[:reconnect_time_wait].nil?
        opts[:max_reconnect_attempts] = MAX_RECONNECT_ATTEMPTS if opts[:max_reconnect_attempts].nil?
        opts[:ping_interval] = DEFAULT_PING_INTERVAL if opts[:ping_interval].nil?
        opts[:max_outstanding_pings] = DEFAULT_PING_MAX if opts[:max_outstanding_pings].nil?

        # Override with ENV
        opts[:verbose] = ENV['NATS_VERBOSE'].downcase == 'true' unless ENV['NATS_VERBOSE'].nil?
        opts[:pedantic] = ENV['NATS_PEDANTIC'].downcase == 'true' unless ENV['NATS_PEDANTIC'].nil?
        opts[:reconnect] = ENV['NATS_RECONNECT'].downcase == 'true' unless ENV['NATS_RECONNECT'].nil?
        opts[:reconnect_time_wait] = ENV['NATS_RECONNECT_TIME_WAIT'].to_i unless ENV['NATS_RECONNECT_TIME_WAIT'].nil?
        opts[:max_reconnect_attempts] = ENV['NATS_MAX_RECONNECT_ATTEMPTS'].to_i unless ENV['NATS_MAX_RECONNECT_ATTEMPTS'].nil?
        opts[:ping_interval] = ENV['NATS_PING_INTERVAL'].to_i unless ENV['NATS_PING_INTERVAL'].nil?
        opts[:max_outstanding_pings] = ENV['NATS_MAX_OUTSTANDING_PINGS'].to_i unless ENV['NATS_MAX_OUTSTANDING_PINGS'].nil?
        opts[:connect_timeout] ||= DEFAULT_CONNECT_TIMEOUT
        @options = opts

        # Process servers in the NATS cluster and pick one to connect
        uris = opts[:servers] || [DEFAULT_URI]
        uris.shuffle! unless @options[:dont_randomize_servers]
        uris.each do |u|
          @server_pool << { :uri => u.is_a?(URI) ? u.dup : URI.parse(u) }
        end

        # Check for TLS usage
        @tls = @options[:tls]

        begin
          current = select_next_server

          # Create TCP socket connection to NATS
          @io = create_socket
          @io.connect

          # Capture state that we have had a TCP connection established against
          # this server and could potentially be used for reconnecting.
          current[:was_connected] = true

          # Connection established and now in process of sending CONNECT to NATS
          @status = CONNECTING

          # Established TCP connection successfully so can start connect
          process_connect_init

          # Reset reconnection attempts if connection is valid
          current[:reconnect_attempts] = 0
        rescue NoServersError => e
          @disconnect_cb.call(e) if @disconnect_cb
          raise @last_err || e
        rescue => e
          # Capture sticky error
          synchronize { @last_err = e }

          @err_cb.call(e) if @err_cb

          if should_not_reconnect?
            @disconnect_cb.call(e) if @disconnect_cb
            raise e
          end

          # Clean up any connecting state and close connection without
          # triggering the disconnection/closed callbacks.
          close_connection(DISCONNECTED, false)

          # always sleep here to safe guard against errors before current[:was_connected]
          # is set for the first time
          sleep @options[:reconnect_time_wait] if @options[:reconnect_time_wait]

          # Continue retrying until there are no options left in the server pool
          retry
        end

        # Initialize queues and loops for message dispatching and processing engine
        @flush_queue = SizedQueue.new(MAX_FLUSH_KICK_SIZE)
        @pending_queue = SizedQueue.new(MAX_PENDING_SIZE)
        @pings_outstanding = 0
        @pongs_received = 0
        @pending_size = 0

        # Server roundtrip went ok so consider to be connected at this point
        @status = CONNECTED

        # Connected to NATS so Ready to start parser loop, flusher and ping interval
        start_threads!
      end

      def publish(subject, msg=EMPTY_MSG, opt_reply=nil, &blk)
        raise BadSubject if !subject or subject.empty?
        msg_size = msg.bytesize

        # Accounting
        @stats[:out_msgs] += 1
        @stats[:out_bytes] += msg_size

        send_command("PUB #{subject} #{opt_reply} #{msg_size}\r\n#{msg}\r\n")
        @flush_queue << :pub if @flush_queue.empty?
      end

      # Create subscription which is dispatched asynchronously
      # messages to a callback.
      def subscribe(subject, opts={}, &callback)
        sid = (@ssid += 1)
        sub = @subs[sid] = Subscription.new
        sub.subject = subject
        sub.callback = callback
        sub.received = 0
        sub.queue = opts[:queue] if opts[:queue]
        sub.max = opts[:max] if opts[:max]

        send_command("SUB #{subject} #{opts[:queue]} #{sid}#{CR_LF}")
        @flush_queue << :sub

        # Setup server support for auto-unsubscribe when receiving enough messages
        unsubscribe(sid, opts[:max]) if opts[:max]

        sid
      end

      # Sends a request expecting a single response or raises a timeout
      # in case the request is not retrieved within the specified deadline.
      # If given a callback, then the request happens asynchronously.
      def request(subject, payload, opts={}, &blk)
        return unless subject
        inbox = new_inbox

        # If a callback was passed, then have it process
        # the messages asynchronously and return the sid.
        if blk
          opts[:max] ||= 1
          s = subscribe(inbox, opts) do |msg, reply|
            case blk.arity
            when 0 then blk.call
            when 1 then blk.call(msg)
            else blk.call(msg, reply)
            end
          end
          publish(subject, payload, inbox)

          return s
        end

        # In case block was not given, handle synchronously
        # with a timeout and only allow a single response.
        timeout = opts[:timeout] ||= 0.5
        opts[:max] = 1

        sub = Subscription.new
        sub.subject = inbox
        sub.received = 0
        future = sub.new_cond
        sub.future = future

        sid = nil
        synchronize do
          sid = (@ssid += 1)
          @subs[sid] = sub
        end

        send_command("SUB #{inbox} #{sid}#{CR_LF}")
        @flush_queue << :sub
        unsubscribe(sid, 1)

        sub.synchronize do
          # Publish the request and then wait for the response...
          publish(subject, payload, inbox)

          with_nats_timeout(timeout) do
            future.wait(timeout)
          end
        end
        response = sub.response

        response
      end

      # Auto unsubscribes the server by sending UNSUB command and throws away
      # subscription in case already present and has received enough messages.
      def unsubscribe(sid, opt_max=nil)
        opt_max_str = " #{opt_max}" unless opt_max.nil?
        send_command("UNSUB #{sid}#{opt_max_str}#{CR_LF}")
        @flush_queue << :unsub

        return unless sub = @subs[sid]
        synchronize do
          sub.max = opt_max
          @subs.delete(sid) unless (sub.max && (sub.received < sub.max))
        end
      end

      # Send a ping and wait for a pong back within a timeout.
      def flush(timeout=60)
        # Schedule sending a PING, and block until we receive PONG back,
        # or raise a timeout in case the response is past the deadline.
        pong = @pongs.new_cond
        @pongs.synchronize do
          @pongs << pong

          # Flush once pong future has been prepared
          @pending_queue << PING_REQUEST
          @flush_queue << :ping
          with_nats_timeout(timeout) do
            pong.wait(timeout)
          end
        end
      end

      alias :servers :server_pool

      def discovered_servers
        servers.select {|s| s[:discovered] }
      end

      # Methods only used by the parser

      def process_pong
        # Take first pong wait and signal any flush in case there was one
        @pongs.synchronize do
          pong = @pongs.pop
          pong.signal unless pong.nil?
        end
        @pings_outstanding -= 1
        @pongs_received += 1
      end

      # Received a ping so respond back with a pong
      def process_ping
        @pending_queue << PONG_RESPONSE
        @flush_queue << :ping
        pong = @pongs.new_cond
        @pongs.synchronize { @pongs << pong }
      end

      # Handles protocol errors being sent by the server.
      def process_err(err)
        # FIXME: In case of a stale connection, then handle as process_op_error

        # In case of permissions violation then dispatch the error callback
        # while holding the lock.
        current = server_pool.first
        current[:error_received] = true
        if current[:auth_required]
          @err_cb.call(NATS::IO::AuthError.new(err))
        else
          @err_cb.call(NATS::IO::ServerError.new(err))
        end

        # Otherwise, capture the error under a lock and close
        # the connection gracefully.
        synchronize do
          @last_err = NATS::IO::ServerError.new(err)
        end

        # Process disconnect under a different thread as reading loop
        Thread.new { close }
      end

      def process_msg(subject, sid, reply, data)
        # Accounting
        @stats[:in_msgs] += 1
        @stats[:in_bytes] += data.size

        # Throw away in case we no longer manage the subscription
        sub = nil
        synchronize { sub = @subs[sid] }
        return unless sub

        # Check for auto_unsubscribe
        sub.synchronize do
          sub.received += 1
          if sub.max
            case
            when sub.received > sub.max
              # Client side support in case server did not receive unsubscribe
              unsubscribe(sid)
              return
            when sub.received == sub.max
              # Cleanup here if we have hit the max..
              @subs.delete(sid)
            end
          end

          # In case of a request which requires a future
          # do so here already while holding the lock and return
          if sub.future
            future = sub.future
            sub.response = Msg.new(subject, reply, data)
            future.signal

            return
          end
        end

        # Distinguish between async subscriptions with callbacks
        # and request subscriptions which expect a single response.
        if sub.callback
          cb = sub.callback
          case cb.arity
          when 0 then cb.call
          when 1 then cb.call(data)
          when 2 then cb.call(data, reply)
          else cb.call(data, reply, subject)
          end
        end
      end

      def process_info(line)
        parsed_info = JSON.parse(line)

        # INFO can be received asynchronously too,
        # so has to be done under the lock.
        synchronize do
          # Symbolize keys from parsed info line
          @server_info = parsed_info.reduce({}) do |info, (k,v)|
            info[k.to_sym] = v

            info
          end

          # Detect any announced server that we might not be aware of...
          connect_urls = @server_info[:connect_urls]
          if connect_urls
            srvs = []
            connect_urls.each do |url|
              u = URI.parse("nats://#{url}")
              present = server_pool.detect do |srv|
                srv[:uri].host == u.host && srv[:uri].port == u.port
              end

              if not present
                # Let explicit user and pass options set the credentials.
                u.user = options[:user] if options[:user]
                u.password = options[:pass] if options[:pass]

                # Use creds from the current server if not set explicitly.
                if @uri
                  u.user ||= @uri.user if @uri.user
                  u.password ||= @uri.password if @uri.password
                end

                srvs << { :uri => u, :reconnect_attempts => 0, :discovered => true }
              end
            end
            srvs.shuffle! unless @options[:dont_randomize_servers]

            # Include in server pool but keep current one as the first one.
            server_pool.push(*srvs)
          end
        end

        @server_info
      end

      # Close connection to NATS, flushing in case connection is alive
      # and there are any pending messages, should not be used while
      # holding the lock.
      def close
        close_connection(CLOSED, true)
      end

      def new_inbox
        "_INBOX.#{SecureRandom.hex(13)}"
      end

      def connected_server
        connected? ? @uri : nil
      end

      def connected?
        @status == CONNECTED
      end

      def connecting?
        @status == CONNECTING
      end

      def reconnecting?
        @status == RECONNECTING
      end

      def closed?
        @status == CLOSED
      end

      def on_error(&callback)
        @err_cb = callback
      end

      def on_disconnect(&callback)
        @disconnect_cb = callback
      end

      def on_reconnect(&callback)
        @reconnect_cb = callback
      end

      def on_close(&callback)
        @close_cb = callback
      end

      def last_error
        synchronize do
          @last_err
        end
      end

      private

      def select_next_server
        raise NoServersError.new("nats: No servers available") if server_pool.empty?

        # Pick next from head of the list
        srv = server_pool.shift

        # Track connection attempts to this server
        srv[:reconnect_attempts] ||= 0
        srv[:reconnect_attempts] += 1

        # In case there was an error from the server we will
        # take it out from rotation unless we specify infinite
        # reconnects via setting :max_reconnect_attempts to -1
        if options[:max_reconnect_attempts] < 0 || can_reuse_server?(srv)
          server_pool << srv
        end

        # Back off in case we are reconnecting to it and have been connected
        sleep @options[:reconnect_time_wait] if should_delay_connect?(srv)

        # Set url of the server to which we would be connected
        @uri = srv[:uri]
        @uri.user = @options[:user] if @options[:user]
        @uri.password = @options[:pass] if @options[:pass]

        srv
      end

      def server_using_secure_connection?
        @server_info[:ssl_required] || @server_info[:tls_required]
      end

      def client_using_secure_connection?
        @uri.scheme == "tls" || @tls
      end

      def send_command(command)
        @pending_size += command.bytesize
        @pending_queue << command

        # TODO: kick flusher here in case pending_size growing large
      end

      def auth_connection?
        !@uri.user.nil?
      end

      def connect_command
        cs = {
          :verbose  => @options[:verbose],
          :pedantic => @options[:pedantic],
          :lang     => NATS::IO::LANG,
          :version  => NATS::IO::VERSION,
          :protocol => NATS::IO::PROTOCOL
        }
        cs[:name] = @options[:name] if @options[:name]

        if auth_connection?
          if @uri.password
            cs[:user] = @uri.user
            cs[:pass] = @uri.password
          else
            cs[:auth_token] = @uri.user
          end
        end

        "CONNECT #{cs.to_json}#{CR_LF}"
      end

      def with_nats_timeout(timeout)
        start_time = MonotonicTime.now
        yield
        end_time = MonotonicTime.now
        duration = end_time - start_time
        raise NATS::IO::Timeout.new("nats: timeout") if duration > timeout
      end

      # Handles errors from reading, parsing the protocol or stale connection.
      # the lock should not be held entering this function.
      def process_op_error(e)
        should_bail = synchronize do
          connecting? || closed? || reconnecting?
        end
        return if should_bail

        synchronize do
          @last_err = e
          @err_cb.call(e) if @err_cb

          # If we were connected and configured to reconnect,
          # then trigger disconnect and start reconnection logic
          if connected? and should_reconnect?
            @status = RECONNECTING
            @io.close if @io
            @io = nil

            # TODO: Reconnecting pending buffer?

            # Do reconnect under a different thread than the one
            # in which we got the error.
            Thread.new do
              begin
                # Abort currently running reads in case they're around
                # FIXME: There might be more graceful way here...
                @read_loop_thread.exit if @read_loop_thread.alive?
                @flusher_thread.exit if @flusher_thread.alive?
                @ping_interval_thread.exit if @ping_interval_thread.alive?

                attempt_reconnect
              rescue NoServersError => e
                @last_err = e
                close
              end
            end

            Thread.exit
            return
          end

          # Otherwise, stop trying to reconnect and close the connection
          @status = DISCONNECTED
        end

        # Otherwise close the connection to NATS
        close
      end

      # Gathers data from the socket and sends it to the parser.
      def read_loop
        loop do
          begin
            should_bail = synchronize do
              # FIXME: In case of reconnect as well?
              @status == CLOSED or @status == RECONNECTING
            end
            if !@io or @io.closed? or should_bail
              return
            end

            # TODO: Remove timeout and just wait to be ready
            data = @io.read(MAX_SOCKET_READ_BYTES)
            @parser.parse(data) if data
          rescue Errno::ETIMEDOUT
            # FIXME: We do not really need a timeout here...
            retry
          rescue => e
            # In case of reading/parser errors, trigger
            # reconnection logic in case desired.
            process_op_error(e)
          end
        end
      end

      # Waits for client to notify the flusher that it will be
      # it is sending a command.
      def flusher_loop
        loop do
          # Blocks waiting for the flusher to be kicked...
          @flush_queue.pop

          should_bail = synchronize do
            @status != CONNECTED || @status == CONNECTING
          end
          return if should_bail

          # Skip in case nothing remains pending already.
          next if @pending_queue.empty?

          # FIXME: should limit how many commands to take at once
          # since producers could be adding as many as possible
          # until reaching the max pending queue size.
          cmds = []
          cmds << @pending_queue.pop until @pending_queue.empty?
          begin
            @io.write(cmds.join) unless cmds.empty?
          rescue => e
            synchronize do
              @last_err = e
              @err_cb.call(e) if @err_cb
            end

            process_op_error(e)
            return
          end if @io

          synchronize do
            @pending_size = 0
          end
        end
      end

      def ping_interval_loop
        loop do
          sleep @options[:ping_interval]

          # Skip ping interval until connected
          next if !connected?

          if @pings_outstanding >= @options[:max_outstanding_pings]
            process_op_error(StaleConnectionError.new("nats: stale connection"))
            return
          end

          @pings_outstanding += 1
          send_command(PING_REQUEST)
          @flush_queue << :ping
        end
      rescue => e
        process_op_error(e)
      end

      def process_connect_init
        line = @io.read_line(options[:connect_timeout])
        _, info_json = line.split(' ')
        process_info(info_json)

        case
        when (server_using_secure_connection? and client_using_secure_connection?)
          tls_context = nil

          if @tls
            # Allow prepared context and customizations via :tls opts
            tls_context = @tls[:context] if @tls[:context]
          else
            # Defaults
            tls_context = OpenSSL::SSL::SSLContext.new
            tls_context.ssl_version = :TLSv1_2
          end

          # Setup TLS connection by rewrapping the socket
          tls_socket = OpenSSL::SSL::SSLSocket.new(@io.socket, tls_context)
          tls_socket.connect
          @io.socket = tls_socket
        when (server_using_secure_connection? and !client_using_secure_connection?)
          raise NATS::IO::ConnectError.new('TLS/SSL required by server')
        when (client_using_secure_connection? and !server_using_secure_connection?)
          raise NATS::IO::ConnectError.new('TLS/SSL not supported by server')
        else
          # Otherwise, use a regular connection.
        end

        if @server_info[:auth_required]
          current = server_pool.first
          current[:auth_required] = true
        end

        # Send connect and process synchronously. If using TLS,
        # it should have handled upgrading at this point.
        @io.write(connect_command)

        # Send ping/pong after connect
        @io.write(PING_REQUEST)

        next_op = @io.read_line(options[:connect_timeout])
        if @options[:verbose]
          # Need to get another command here if verbose
          raise NATS::IO::ConnectError.new("expected to receive +OK") unless next_op =~ NATS::Protocol::OK
          next_op = @io.read_line(options[:connect_timeout])
        end

        case next_op
        when NATS::Protocol::PONG
        when NATS::Protocol::ERR
          if @server_info[:auth_required]
            raise NATS::IO::AuthError.new($1)
          else
            raise NATS::IO::ServerError.new($1)
          end
        else
          raise NATS::IO::ConnectError.new("expected PONG, got #{next_op}")
        end
      end

      # Reconnect logic, this is done while holding the lock.
      def attempt_reconnect
        @disconnect_cb.call(@last_err) if @disconnect_cb

        # Clear sticky error
        @last_err = nil

        # Do reconnect
        begin
          current = select_next_server

          # Establish TCP connection with new server
          @io = create_socket
          @io.connect
          @stats[:reconnects] += 1

          # Established TCP connection successfully so can start connect
          process_connect_init

          # Reset reconnection attempts if connection is valid
          current[:reconnect_attempts] = 0
        rescue NoServersError => e
          raise e
        rescue => e
          @last_err = e

          # Trigger async error handler
          @err_cb.call(e) if @err_cb

          # Continue retrying until there are no options left in the server pool
          retry
        end

        # Clear pending flush calls and reset state before restarting loops
        @flush_queue.clear
        @pings_outstanding = 0
        @pongs_received = 0

        # Replay all subscriptions
        @subs.each_pair do |sid, sub|
          @io.write("SUB #{sub.subject} #{sub.queue} #{sid}#{CR_LF}")
        end

        # Flush anything which was left pending, in case of errors during flush
        # then we should raise error then retry the reconnect logic
        cmds = []
        cmds << @pending_queue.pop until @pending_queue.empty?
        @io.write(cmds.join) unless cmds.empty?
        @status = CONNECTED
        @pending_size = 0

        # Now connected to NATS, and we can restart parser loop, flusher
        # and ping interval
        start_threads!

        # Dispatch the reconnected callback while holding lock
        # which we should have already
        @reconnect_cb.call if @reconnect_cb
      end

      def close_connection(conn_status, do_cbs=true)
        synchronize do
          if @status == CLOSED
            @status = conn_status
            return
          end
        end

        # Kick the flusher so it bails due to closed state
        @flush_queue << :fallout if @flush_queue
        Thread.pass

        # FIXME: More graceful way of handling the following?
        # Ensure ping interval and flusher are not running anymore
        if @ping_interval_thread and @ping_interval_thread.alive?
          @ping_interval_thread.exit
        end

        if @flusher_thread and @flusher_thread.alive?
          @flusher_thread.exit
        end

        if @read_loop_thread and @read_loop_thread.alive?
          @read_loop_thread.exit
        end

        # TODO: Delete any other state which we are not using here too.
        synchronize do
          @pongs.synchronize do
            @pongs.each do |pong|
              pong.signal
            end
            @pongs.clear
          end

          # Try to write any pending flushes in case
          # we have a connection then close it.
          should_flush = (@pending_queue && @io && @io.socket && !@io.closed?)
          begin
            cmds = []
            cmds << @pending_queue.pop until @pending_queue.empty?

            # FIXME: Fails when empty on TLS connection?
            @io.write(cmds.join) unless cmds.empty?
          rescue => e
            @last_err = e
            @err_cb.call(e) if @err_cb
          end if should_flush

          # TODO: Destroy any remaining subscriptions
          if do_cbs
            @disconnect_cb.call(@last_err) if @disconnect_cb
            @close_cb.call if @close_cb
          end

          @status = conn_status

          # Close the established connection in case
          # we still have it.
          if @io
            @io.close if @io.socket
            @io = nil
          end
        end
      end

      def start_threads!
        # Reading loop for gathering data
        @read_loop_thread = Thread.new { read_loop }
        @read_loop_thread.abort_on_exception = true

        # Flusher loop for sending commands
        @flusher_thread = Thread.new { flusher_loop }
        @flusher_thread.abort_on_exception = true

        # Ping interval handling for keeping alive the connection
        @ping_interval_thread = Thread.new { ping_interval_loop }
        @ping_interval_thread.abort_on_exception = true
      end

      def can_reuse_server?(server)
        # We will retry a number of times to reconnect to a server
        # unless we got a hard error from it already.
        server[:reconnect_attempts] <= @options[:max_reconnect_attempts] && !server[:error_received]
      end

      def should_delay_connect?(server)
        server[:was_connected] && server[:reconnect_attempts] >= 0
      end

      def should_not_reconnect?
        !@options[:reconnect]
      end

      def should_reconnect?
        @options[:reconnect]
      end

      def create_socket
        NATS::IO::Socket.new({
          uri: @uri,
          connect_timeout: DEFAULT_CONNECT_TIMEOUT
        })
      end
    end

    # Implementation adapted from https://github.com/redis/redis-rb
    class Socket
      attr_accessor :socket

      # Exceptions raised during non-blocking I/O ops that require retrying the op
      NBIO_READ_EXCEPTIONS  = [Errno::EWOULDBLOCK, Errno::EAGAIN, ::IO::WaitReadable]
      NBIO_WRITE_EXCEPTIONS = [Errno::EWOULDBLOCK, Errno::EAGAIN, ::IO::WaitWritable]

      def initialize(options={})
        @uri = options[:uri]
        @connect_timeout = options[:connect_timeout]
        @write_timeout = options[:write_timeout]
        @read_timeout = options[:read_timeout]
        @socket = nil
      end

      def connect
        addrinfo = ::Socket.getaddrinfo(@uri.host, nil, ::Socket::AF_UNSPEC, ::Socket::SOCK_STREAM)
        addrinfo.each_with_index do |ai, i|
          begin
            @socket = connect_addrinfo(ai, @uri.port, @connect_timeout)
            break
          rescue SystemCallError => e
            # Give up if no more available
            raise e if addrinfo.length == i+1
          end
        end

        # Set TCP no delay by default
        @socket.setsockopt(::Socket::IPPROTO_TCP, ::Socket::TCP_NODELAY, 1)
      end

      def read_line(deadline=nil)
        # FIXME: Should accumulate and read in a non blocking way instead
        unless ::IO.select([@socket], nil, nil, deadline)
          raise SocketTimeoutError
        end
        @socket.gets
      end

      def read(max_bytes, deadline=nil)

        begin
          return @socket.read_nonblock(max_bytes)
        rescue *NBIO_READ_EXCEPTIONS
          if ::IO.select([@socket], nil, nil, deadline)
            retry
          else
            raise SocketTimeoutError
          end
        rescue *NBIO_WRITE_EXCEPTIONS
          if ::IO.select(nil, [@socket], nil, deadline)
            retry
          else
            raise SocketTimeoutError
          end
        end
      rescue EOFError => e
        if RUBY_ENGINE == 'jruby' and e.message == 'No message available'
          # FIXME: <EOFError: No message available> can happen in jruby
          # even though seems it is temporary and eventually possible
          # to read from socket.
          return nil
        end
        raise Errno::ECONNRESET
      end

      def write(data, deadline=nil)
        length = data.bytesize
        total_written = 0

        loop do
          begin
            written = @socket.write_nonblock(data)

            total_written += written
            break total_written if total_written >= length
            data = data.byteslice(written..-1)
          rescue *NBIO_WRITE_EXCEPTIONS
            if ::IO.select(nil, [@socket], nil, deadline)
              retry
            else
              raise SocketTimeoutError
            end
          rescue *NBIO_READ_EXCEPTIONS => e
            if ::IO.select([@socket], nil, nil, deadline)
              retry
            else
              raise SocketTimeoutError
            end
          end
        end

      rescue EOFError
        raise Errno::ECONNRESET
      end

      def close
        @socket.close
      end

      def closed?
        @socket.closed?
      end

      private

      def connect_addrinfo(ai, port, timeout)
        sock = ::Socket.new(::Socket.const_get(ai[0]), ::Socket::SOCK_STREAM, 0)
        sockaddr = ::Socket.pack_sockaddr_in(port, ai[3])

        begin
          sock.connect_nonblock(sockaddr)
        rescue Errno::EINPROGRESS, Errno::EALREADY, ::IO::WaitWritable
          unless ::IO.select(nil, [sock], nil, @connect_timeout)
            raise SocketTimeoutError
          end

          # Confirm that connection was established
          begin
            sock.connect_nonblock(sockaddr)
          rescue Errno::EISCONN
            # Connection was established without issues.
          end
        end

        sock
      end
    end
  end

  Msg = Struct.new(:subject, :reply, :data)

  class Subscription
    include MonitorMixin

    attr_accessor :subject, :queue, :future, :callback, :response, :received, :max

    def initialize
      super # required to initialize monitor
      @subject  = ''
      @queue    = nil
      @future   = nil
      @callback = nil
      @response = nil
      @received = 0
      @max      = nil
    end
  end

  # Implementation of MonotonicTime adapted from
  # https://github.com/ruby-concurrency/concurrent-ruby/
  class MonotonicTime
    class << self
      case
      when defined?(Process::CLOCK_MONOTONIC)
        def now
          Process.clock_gettime(Process::CLOCK_MONOTONIC)
        end
      when RUBY_ENGINE == 'jruby'
        def now
          java.lang.System.nanoTime() / 1_000_000_000.0
        end
      else
        def now
          # Fallback to regular time behavior
          ::Time.now.to_f
        end
      end
    end
  end
end
