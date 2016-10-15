require 'nats/io/parser'
require 'nats/io/version'
require 'thread'
require 'socket'
require 'json'
require 'monitor'
require 'uri'
require 'securerandom'

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

    # When the NATS server sends us an ERROR message, this is raised/passed by default
    class ServerError < Error; end

    # When we detect error on the client side (e.g. Fast Producer, TLS required)
    class ClientError < Error; end

    # When we cannot connect to the server (either initially or after a reconnect), this is raised/passed
    class ConnectError < Error; end

    # When we cannot connect to the server because authorization failed.
    class AuthError < ConnectError; end

    # When we do not get a result within a specified time.
    class Timeout < Error; end

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

        # TODO: connect, reconnect, close, disconnect callbacks
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

        @err_cb = proc { |e| raise e }
        @close_cb = proc { }
        @disconnect_cb = proc { }
      end

      # Establishes connection to NATS
      def connect(opts={})
        opts[:verbose] = false if opts[:verbose].nil?
        opts[:pedantic] = false if opts[:pedantic].nil?
        opts[:reconnect] = true if opts[:reconnect].nil?
        opts[:max_reconnect_attempts] = MAX_RECONNECT_ATTEMPTS if opts[:max_reconnect_attempts].nil?
        opts[:reconnect_time_wait] = RECONNECT_TIME_WAIT if opts[:reconnect_time_wait].nil?
        opts[:ping_interval] = DEFAULT_PING_INTERVAL if opts[:ping_interval].nil?
        opts[:max_outstanding_pings] = DEFAULT_PING_MAX if opts[:max_outstanding_pings].nil?

        # Override with ENV
        opts[:verbose] = ENV['NATS_VERBOSE'].downcase == 'true' unless ENV['NATS_VERBOSE'].nil?
        opts[:pedantic] = ENV['NATS_PEDANTIC'].downcase == 'true' unless ENV['NATS_PEDANTIC'].nil?
        opts[:debug] = ENV['NATS_DEBUG'].downcase == 'true' unless ENV['NATS_DEBUG'].nil?
        opts[:reconnect] = ENV['NATS_RECONNECT'].downcase == 'true' unless ENV['NATS_RECONNECT'].nil?
        opts[:fast_producer_error] = ENV['NATS_FAST_PRODUCER'].downcase == 'true' unless ENV['NATS_FAST_PRODUCER'].nil?
        opts[:ssl] = ENV['NATS_SSL'].downcase == 'true' unless ENV['NATS_SSL'].nil?
        opts[:max_reconnect_attempts] = ENV['NATS_MAX_RECONNECT_ATTEMPTS'].to_i unless ENV['NATS_MAX_RECONNECT_ATTEMPTS'].nil?
        opts[:reconnect_time_wait] = ENV['NATS_RECONNECT_TIME_WAIT'].to_i unless ENV['NATS_RECONNECT_TIME_WAIT'].nil?
        opts[:ping_interval] = ENV['NATS_PING_INTERVAL'].to_i unless ENV['NATS_PING_INTERVAL'].nil?
        opts[:max_outstanding_pings] = ENV['NATS_MAX_OUTSTANDING_PINGS'].to_i unless ENV['NATS_MAX_OUTSTANDING_PINGS'].nil?
        @options = opts

        # Process servers in the NATS cluster and pick one to connect
        uris = opts[:servers] || [DEFAULT_URI]
        uris.shuffle! unless @options[:dont_randomize_servers]
        uris.each { |u| @server_pool << { :uri => u.is_a?(URI) ? u.dup : URI.parse(u) } }
        bind_primary

        # TODO: TLS options

        # Create TCP socket connection to NATS.
        # TODO: Make this tunable?
        @io = create_socket
        @io.connect

        # Capture state that we have had a TCP connection established against
        # this server and could potentially be used for reconnecting.
        current = server_pool.first
        current[:was_connected] = true
        current[:reconnect_attempts] = 0

        # Connection established and now in process of sending CONNECT to NATS
        @status = CONNECTING

        line = @io.read_line
        _, info_json = line.split(' ')
        process_info(info_json)

        # TODO: Handle upgrading connection to TLS secure connection

        process_connect_init

        # Server roundtrip went ok so consider to be connected at this point
        @status = CONNECTED

        # Initialize queues and loops for message dispatching and processing engine
        @flush_queue = SizedQueue.new(MAX_FLUSH_KICK_SIZE)
        @pending_queue = SizedQueue.new(MAX_PENDING_SIZE)

        # Reading loop for gathering data
        @read_loop_thread = Thread.new { read_loop }
        @read_loop_thread.abort_on_exception = true

        # Flusher loop for sending commands
        @flusher_thread = Thread.new { flusher_loop }
        @flusher_thread.abort_on_exception = true

        # Ping interval handling for keeping alive the connection
        @pings_outstanding = 0
        @pongs_received = 0
        @ping_interval_thread = Thread.new { ping_interval_loop }
        @ping_interval_thread.abort_on_exception = true
      end

      def publish(subject, msg=EMPTY_MSG, opt_reply=nil, &blk)
        msg_size = msg.bytesize
        send_command("PUB #{subject} #{opt_reply} #{msg_size}\r\n#{msg}\r\n")

        @stats[:out_msgs] += 1
        @stats[:out_bytes] += msg_size
        @flush_queue << :pub if @flush_queue.empty?
      end

      # Create subscription which is dispatched asynchronously
      # messages to a callback.
      def subscribe(subject, opts={}, &callback)
        return unless subject
        sid = (@ssid += 1)
        sub = @subs[sid] = { :subject => subject, :callback => callback, :received => 0 }
        sub[:queue] = opts[:queue] if opts[:queue]
        sub[:max] = opts[:max] if opts[:max]

        send_command("SUB #{subject} #{opts[:queue]} #{sid}#{CR_LF}")
        @flush_queue << :sub

        # Setup server support for auto-unsubscribe
        unsubscribe(sid, opts[:max]) if opts[:max]

        sid
      end

      # Creates a requests which is handled asynchronously via a callback.
      def request(subject, data=nil, opts={}, &cb)
        return unless subject
        inbox = new_inbox
        s = subscribe(inbox, opts) do |msg, reply|
          case cb.arity
          when 0 then cb.call
          when 1 then cb.call(msg)
          else cb.call(msg, reply)
          end
        end
        publish(subject, data, inbox)
      end

      # Sends a request expecting a single response or raises a timeout
      # in case the request is not retrieved within the specified deadline.
      def timed_request(subject, payload, timeout=0.5)
        sub = Subscription.new
        inbox = new_inbox
        future = sub.new_cond
        sub[:subject]  = inbox
        sub[:future]   = future
        sub[:received] = 0

        sid = (@ssid += 1)
        @subs[sid] = sub
        send_command("SUB #{inbox} #{sid}#{CR_LF}")
        @flush_queue << :sub
        unsubscribe(sid, 1)

        # Publish the request and wait for the response...
        publish(subject, payload, inbox)
        sub.synchronize do
          with_nats_timeout(timeout) do
            future.wait(timeout)
          end
        end
        response = sub[:response]

        response
      end

      # Auto unsubscribes the server sending UNSUB
      def unsubscribe(sid, opt_max=nil)
        opt_max_str = " #{opt_max}" unless opt_max.nil?
        send_command("UNSUB #{sid}#{opt_max_str}#{CR_LF}")
        @flush_queue << :unsub

        return unless sub = @subs[sid]
        sub[:max] = opt_max
        @subs.delete(sid) unless (sub[:max] && (sub[:received] < sub[:max]))
      end

      # Send a ping and wait for a pong back within a timeout.
      def flush(timeout=60)
        # Schedule sending a PING, and block until we receive PONG back,
        # or raise a timeout in case the response is past the deadline.
        @pending_queue << PING_REQUEST
        @flush_queue << :ping
        pong = @pongs.new_cond
        @pongs.synchronize do
          @pongs << pong
          with_nats_timeout(timeout) do
            pong.wait(timeout)
          end
        end
      end

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
        # TODO: In case of a stale connection, then handle as process op err.

        # In case of permissions violation then dispatch the error callback
        # while holding the lock.
        current = server_pool.first
        current[:error_received] = true
        if current[:auth_required]
          @err_cb.call(NATS::IO::AuthError.new(err))
        else
          @err_cb.call(NATS::IO::ServerError.new(err))
        end

        # Otherwise, capture the error under a lock and close the connection gracefully.
        synchronize do
          @last_err = NATS::IO::ServerError.new(err)
        end

        close
      end

      def process_msg(subject, sid, reply, msg)
        return unless sub = @subs[sid]

        # Check for auto_unsubscribe
        sub[:received] += 1
        if sub[:max]
          # Client side support in case server did not receive unsubscribe
          return unsubscribe(sid) if (sub[:received] > sub[:max])

          # Cleanup here if we have hit the max..
          @subs.delete(sid) if (sub[:received] == sub[:max])
        end

        # Distinguish between async subscriptions with callbacks
        # and request subscriptions which expect a single response.
        if sub[:callback]
          cb = sub[:callback]
          case cb.arity
          when 0 then cb.call
          when 1 then cb.call(msg)
          when 2 then cb.call(msg, reply)
          else cb.call(msg, reply, subject)
          end
        elsif sub[:future]
          future = sub[:future]
          sub[:response] = { :subject => subject, :reply => reply, :data => msg }
          sub.synchronize do
            future.signal
          end
        end
      end

      # Close connection to NATS, blocking in case connection is alive
      # and there are any pending messages.
      def close
        # Should only close once...
        should_bail = synchronize do
          return true if @status == CLOSED
          @status = CLOSED

          false
        end
        return if should_bail

        # Kick the flusher so it bails due to closed state
        @flush_queue << :fallout
        Thread.pass

        # Ensure ping interval and flusher are not running anymore
        @ping_interval_thread.kill if @ping_interval_thread.alive?
        @flusher_thread.kill if @flusher_thread.alive?

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
          begin
            cmds = []
            cmds << @pending_queue.pop until @pending_queue.empty?
            @io.write(cmds.join)
          rescue => e
            @err_cb.call(e) if @err_cb
          end if @io

          # TODO: Destroy any remaining subscriptions
          @disconnect_cb.call if @disconnect_cb
          @close_cb.call if @close_cb
        end

        # Close the established connection in case
        # we still have it.
        @io.close if @io
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

      def bind_primary
        first = server_pool.first
        @uri = first[:uri]
        @uri.user = @options[:user] if @options[:user]
        @uri.password = @options[:pass] if @options[:pass]
        first
      end

      def process_info(info) #:nodoc:
        # Each JSON parser uses a different key/value pair to use symbol keys
        # instead of strings when parsing. Passing all three pairs assures each
        # parser gets what it needs. For the json gem :symbolize_name, for yajl
        # :symbolize_keys, and for oj :symbol_keys.
        @server_info = JSON.parse(info, :symbolize_keys => true, :symbolize_names => true, :symbol_keys => true)

        # TODO: Handle upgrading connection to TLS secure connection

        if server_using_secure_connection?
          @err_cb.call(NATS::IO::ClientError.new('TLS/SSL required by server'))
          return
        end

        if @server_info[:auth_required]
          current = server_pool.first
          current[:auth_required] = true
        end

        @server_info
      end

      def server_using_secure_connection?
        @server_info[:ssl_required] || @server_info[:tls_required]
      end

      def send_command(command)
        @pending_size += command.bytesize
        @pending_queue << command

        # TODO: kick flusher here in case pending_size growing large
      end

      def auth_connection?
        !@uri.user.nil?
      end

      def connect_command #:nodoc:
        cs = {
          :verbose  => @options[:verbose],
          :pedantic => @options[:pedantic],
          :lang     => 'ruby-pure',
          :version  => NATS::IO::VERSION
        }
        if auth_connection?
          cs[:user] = @uri.user
          cs[:pass] = @uri.password
        end

        # TODO: TLS options

        "CONNECT #{cs.to_json}#{CR_LF}"
      end

      def with_nats_timeout(timeout)
        start_time = Time.now
        yield
        end_time = Time.now
        duration = end_time - start_time
        raise NATS::IO::Timeout.new if end_time - start_time > timeout
      end

      # Handles errors from reading, parsing the protocol or stale connection.
      # the lock should not be held entering this function.
      def process_op_error(e)
        should_bail = synchronize do
          connecting? || closed? || reconnecting?
        end
        return if should_bail

        synchronize do
          # If we were connected and configured to reconnect,
          # then trigger disconnection and start reconnection logic
          if @options[:reconnect] and connected?
            @status = RECONNECTING

            # Stop ping timer
            @ping_interval_thread.kill if @ping_interval_thread.alive?

            # Close socket if present
            @io.close if @io

            # TODO: Reconnecting pending buffer?

            # Do reconnect
            attempt_reconnect
            return
          end

          @status = DISCONNECTED
          @last_err = e
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
              return true if @status == CLOSED
            end
            return if should_bail

            data = @io.read(MAX_SOCKET_READ_BYTES)
            @parser.parse(data)
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
          # since producers could be adding as much as possible
          # until reaching the max pending queue size.
          synchronize do
            cmds = []
            cmds << @pending_queue.pop until @pending_queue.empty?
            @io.write(cmds.join)
            @pending_size = 0
          end
        end
      end

      def ping_interval_loop
        loop do
          sleep @options[:ping_interval]
          if @pings_outstanding > @options[:max_outstanding_pings]
            # FIXME: Check if we have to dispatch callbacks.
            close
          end

          @pings_outstanding += 1

          send_command(PING_REQUEST)
          @flush_queue << :ping
        end
      rescue => e
        process_op_error(e)
      end

      # Reconnect logic, this is done while holding the lock.
      def attempt_reconnect

        # FIXME: There might be more graceful way here...
        @flusher_thread.kill if @flusher_thread.alive?

        @disconnect_cb.call(@last_err) if @disconnect_cb

        # Clear sticky error
        @last_err = nil

        # Rotate server to use
        current = server_pool.shift

        # In case there was an error from the server we will take it out from rotation
        # unless we specify infinite reconnects via setting :max_reconnect_attempts to -1
        if current && (options[:max_reconnect_attempts] < 0 || can_reuse_server?(current))
          server_pool << current
        end

        bind_primary
        current = server_pool.first
        current[:reconnect_attempts] ||= 0
        current[:reconnect_attempts] += 1
        sleep @options[:reconnect_time_wait] if current[:was_connected]

        # Establish TCP connection with new server
        @io = create_socket
        @io.connect

        line = @io.read_line
        _, info_json = line.split(' ')
        process_info(info_json)

        # TODO: Handle upgrading connection to TLS secure connection

        process_connect_init

        # Now connected, so restart flusher and ping interval
        @flusher_thread = Thread.new { flusher_loop }
        @flusher_thread.abort_on_exception = true
        @flush_queue.clear

        @pings_outstanding = 0
        @pongs_received = 0
        @pongs.clear
        @ping_interval_thread = Thread.new { ping_interval_loop }
        @ping_interval_thread.abort_on_exception = true

        # Replay all subscriptions
        @subs.each_pair do |k, v|
          @io.write("SUB #{v[:subject]} #{v[:queue]} #{k}#{CR_LF}")
        end

        # Flush anything which was left pending.
        # TODO: In case of errors during flush then we should raise error
        # then retry the reconnect logic with another server
        cmds = []
        cmds << @pending_queue.pop until @pending_queue.empty?
        @io.write(cmds.join)
        @pending_size = 0
        @status = CONNECTED

        @reconnect_cb.call if @reconnect_cb
      end

      def can_reuse_server?(server) #:nodoc:
        # If we will retry a number of times to reconnect to a server
        # unless we got an error from it already.
        reconnecting? && server[:reconnect_attempts] <= @options[:max_reconnect_attempts] && !server[:error_received]
      end

      def process_connect_init

        # Send connect and process synchronously
        @io.write(connect_command)

        # Send ping/pong after connect
        @io.write(PING_REQUEST)

        next_op = @io.read_line
        if @options[:verbose]
          # Need to get another command here if verbose
          raise NATS::IO::ConnectError.new("expected to receive +OK") unless next_op =~ NATS::Protocol::OK
          next_op = @io.read_line
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

      def create_socket
        NATS::IO::Socket.new({
          :uri             => @uri,
          :connect_timeout => DEFAULT_CONNECT_TIMEOUT,
          :read_timeout    => DEFAULT_READ_WRITE_TIMEOUT,
          :write_timeout   => DEFAULT_READ_WRITE_TIMEOUT
        })
      end
    end

    class Subscription < Hash
      include MonitorMixin
    end

    class Socket

      def initialize(options={})
        @uri = options[:uri]
        @connect_timeout = options[:connect_timeout]
        @write_timeout = options[:write_timeout]
        @read_timeout = options[:read_timeout]

        addr = ::Socket.getaddrinfo(@uri.host, nil)
        @sockaddr = ::Socket.pack_sockaddr_in(@uri.port, addr[0][3])
        @socket = ::Socket.new(::Socket.const_get(addr[0][0]), ::Socket::SOCK_STREAM, 0)

        # Set TCP no delay by default
        @socket.setsockopt(::Socket::IPPROTO_TCP, ::Socket::TCP_NODELAY, 1)
      end

      def connect
        return @socket.connect(sockaddr) unless @connect_timeout

        begin
          @socket.connect_nonblock(@sockaddr)
        rescue Errno::EINPROGRESS
          raise Errno::ETIMEDOUT unless ::IO.select(nil, [@socket], nil, @connect_timeout)

          # Confirm that connection was established
          begin
            @socket.connect_nonblock(@sockaddr)
          rescue Errno::EISCONN
            # Connection was established without issues.
          end
        end
      end

      def write(bytes)
        raise Errno::ETIMEDOUT unless ::IO.select(nil, [@socket], nil, @write_timeout)
        @socket.write(bytes)
      end

      def read_line
        raise Errno::ETIMEDOUT unless ::IO.select(nil, [@socket], nil, @read_timeout)
        @socket.gets
      end

      def read(max_bytes)
        raise Errno::ETIMEDOUT unless ::IO.select(nil, [@socket], nil, @read_timeout)
        data, _ = @socket.recvfrom(max_bytes)

        data
      end

      def close
        @socket.close
      end
    end
  end
end
