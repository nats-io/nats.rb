# Copyright 2016-2021 The NATS Authors
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

require_relative 'parser'
require_relative 'version'
require_relative 'errors'
require_relative 'msg'
require_relative 'subscription'
require_relative 'jetstream'

require 'nats/nuid'
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
  class << self
    # NATS.connect creates a connection to the NATS Server.
    # @param uri [String] URL endpoint of the NATS Server or cluster.
    # @param opts [Hash] Options to customize the NATS connection.
    # @return [NATS::Client]
    #
    # @example
    #   require 'nats'
    #   nc = NATS.connect("demo.nats.io")
    #   nc.publish("hello", "world")
    #   nc.close
    #
    def connect(uri=nil, opts={})
      nc = NATS::Client.new
      nc.connect(uri, opts)

      nc
    end
  end

  # Status represents the different states from a NATS connection.
  # A client starts from the DISCONNECTED state to CONNECTING during
  # the initial connect, then CONNECTED.  If the connection is reset
  # then it goes from DISCONNECTED to RECONNECTING until it is back to
  # the CONNECTED state.  In case the client gives up reconnecting or
  # the connection is manually closed then it will reach the CLOSED
  # connection state after which it will not reconnect again.
  module Status
    # When the client is not actively connected.
    DISCONNECTED = 0

    # When the client is connected.
    CONNECTED = 1

    # When the client will no longer attempt to connect to a NATS Server.
    CLOSED = 2

    # When the client has disconnected and is attempting to reconnect.
    RECONNECTING = 3

    # When the client is attempting to connect to a NATS Server for the first time.
    CONNECTING = 4

    # When the client is draining a connection before closing.
    DRAINING_SUBS = 5
    DRAINING_PUBS = 6
  end

  # Fork Detection handling
  # Based from similar approach as mperham/connection_pool: https://github.com/mperham/connection_pool/pull/166
  if Process.respond_to?(:fork) && Process.respond_to?(:_fork) # MRI 3.1+
    module ForkTracker
      def _fork
        super.tap do |pid|
          Client.after_fork if pid.zero? # in the child process
        end
      end
    end
    Process.singleton_class.prepend(ForkTracker)
  end

  # Client creates a connection to the NATS Server.
  class Client
    include MonitorMixin
    include Status

    attr_reader :status, :server_info, :server_pool, :options, :connected_server, :stats, :uri

    DEFAULT_PORT = 4222
    DEFAULT_URI = ("nats://localhost:#{DEFAULT_PORT}".freeze)

    CR_LF = ("\r\n".freeze)
    CR_LF_SIZE = (CR_LF.bytesize)

    PING_REQUEST  = ("PING#{CR_LF}".freeze)
    PONG_RESPONSE = ("PONG#{CR_LF}".freeze)

    NATS_HDR_LINE  = ("NATS/1.0#{CR_LF}".freeze)
    STATUS_MSG_LEN = 3
    STATUS_HDR     = ("Status".freeze)
    DESC_HDR       = ("Description".freeze)
    NATS_HDR_LINE_SIZE = (NATS_HDR_LINE.bytesize)

    SUB_OP = ('SUB'.freeze)
    EMPTY_MSG = (''.freeze)

    INSTANCES = ObjectSpace::WeakMap.new # tracks all alive client instances
    private_constant :INSTANCES

    class << self
      # Re-establish connection in a new process after forking to start new threads.
      def after_fork
        INSTANCES.each do |client|
          if client.options[:reconnect]
            was_connected = !client.disconnected?
            client.send(:close_connection, Status::DISCONNECTED, true)
            client.connect if was_connected
          else
            client.send(:err_cb_call, self, NATS::IO::ForkDetectedError, nil)
            client.close
          end
        rescue => e
          warn "nats: Error during handling after_fork callback: #{e}" # TODO: Report as async error via error callback?
        end
      end
    end

    def initialize(uri = nil, opts = {})
      super() # required to initialize monitor
      @initial_uri = uri
      @initial_options = opts

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

      @status = nil

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

      # Hostname of current server; used for when TLS host
      # verification is enabled.
      @hostname = nil
      @single_url_connect_used = false

      # Track whether connect has been already been called.
      @connect_called = false

      # New style request/response implementation.
      @resp_sub = nil
      @resp_map = nil
      @resp_sub_prefix = nil
      @nuid = NATS::NUID.new

      # NKEYS
      @user_credentials = nil
      @nkeys_seed = nil
      @user_nkey_cb = nil
      @user_jwt_cb = nil
      @signature_cb = nil

      # Tokens
      @auth_token = nil

      @inbox_prefix = "_INBOX"

      # Draining
      @drain_t = nil

      # Prepare for calling connect or automatic delayed connection
      parse_and_validate_options if uri || opts.any?

      # Keep track of all client instances to handle them after process forking in Ruby 3.1+
      INSTANCES[self] = self if !defined?(Ractor) || Ractor.current == Ractor.main # Ractors doesn't work in forked processes
    end

    # Prepare connecting to NATS, but postpone real connection until first usage.
    def connect(uri=nil, opts={})
      if uri || opts.any?
        @initial_uri = uri
        @initial_options = opts
      end

      synchronize do
        # In case it has been connected already, then do not need to call this again.
        return if @connect_called
        @connect_called = true
      end

      parse_and_validate_options
      establish_connection!

      self
    end

    private def parse_and_validate_options
      # Reset these in case we have reconnected via fork.
      @server_pool = []
      @resp_sub = nil
      @resp_map = nil
      @resp_sub_prefix = nil
      @nuid = NATS::NUID.new
      @stats = {
        in_msgs: 0,
        out_msgs: 0,
        in_bytes: 0,
        out_bytes: 0,
        reconnects: 0
      }
      @status = DISCONNECTED

      # Convert URI to string if needed.
      uri = @initial_uri.dup
      uri = uri.to_s if uri.is_a?(URI)

      opts = @initial_options.dup

      case uri
      when String
        # Initialize TLS defaults in case any url is using it.
        srvs = opts[:servers] = process_uri(uri)
        if srvs.any? {|u| u.scheme == 'tls'} and !opts[:tls]
          tls_context = OpenSSL::SSL::SSLContext.new
          tls_context.set_params
          opts[:tls] = {
            context: tls_context
          }
        end
        @single_url_connect_used = true if srvs.size == 1
      when Hash
        opts = uri
      end

      opts[:verbose] = false if opts[:verbose].nil?
      opts[:pedantic] = false if opts[:pedantic].nil?
      opts[:reconnect] = true if opts[:reconnect].nil?
      opts[:old_style_request] = false if opts[:old_style_request].nil?
      opts[:ignore_discovered_urls] = false if opts[:ignore_discovered_urls].nil?
      opts[:reconnect_time_wait] = NATS::IO::RECONNECT_TIME_WAIT if opts[:reconnect_time_wait].nil?
      opts[:max_reconnect_attempts] = NATS::IO::MAX_RECONNECT_ATTEMPTS if opts[:max_reconnect_attempts].nil?
      opts[:ping_interval] = NATS::IO::DEFAULT_PING_INTERVAL if opts[:ping_interval].nil?
      opts[:max_outstanding_pings] = NATS::IO::DEFAULT_PING_MAX if opts[:max_outstanding_pings].nil?

      # Override with ENV
      opts[:verbose] = ENV['NATS_VERBOSE'].downcase == 'true' unless ENV['NATS_VERBOSE'].nil?
      opts[:pedantic] = ENV['NATS_PEDANTIC'].downcase == 'true' unless ENV['NATS_PEDANTIC'].nil?
      opts[:reconnect] = ENV['NATS_RECONNECT'].downcase == 'true' unless ENV['NATS_RECONNECT'].nil?
      opts[:reconnect_time_wait] = ENV['NATS_RECONNECT_TIME_WAIT'].to_i unless ENV['NATS_RECONNECT_TIME_WAIT'].nil?
      opts[:ignore_discovered_urls] = ENV['NATS_IGNORE_DISCOVERED_URLS'].downcase == 'true' unless ENV['NATS_IGNORE_DISCOVERED_URLS'].nil?
      opts[:max_reconnect_attempts] = ENV['NATS_MAX_RECONNECT_ATTEMPTS'].to_i unless ENV['NATS_MAX_RECONNECT_ATTEMPTS'].nil?
      opts[:ping_interval] = ENV['NATS_PING_INTERVAL'].to_i unless ENV['NATS_PING_INTERVAL'].nil?
      opts[:max_outstanding_pings] = ENV['NATS_MAX_OUTSTANDING_PINGS'].to_i unless ENV['NATS_MAX_OUTSTANDING_PINGS'].nil?
      opts[:connect_timeout] ||= NATS::IO::DEFAULT_CONNECT_TIMEOUT
      opts[:drain_timeout] ||= NATS::IO::DEFAULT_DRAIN_TIMEOUT
      @options = opts

      # Process servers in the NATS cluster and pick one to connect
      uris = opts[:servers] || [DEFAULT_URI]
      uris.shuffle! unless @options[:dont_randomize_servers]
      uris.each do |u|
        nats_uri = case u
                   when URI
                     u.dup
                   else
                     URI.parse(u)
                   end
        @server_pool << {
          :uri => nats_uri,
          :hostname => nats_uri.hostname
        }
      end

      if @options[:old_style_request]
        # Replace for this instance the implementation
        # of request to use the old_request style.
        class << self; alias_method :request, :old_request; end
      end

      # NKEYS
      @signature_cb ||= opts[:user_signature_cb]
      @user_jwt_cb ||= opts[:user_jwt_cb]
      @user_nkey_cb ||= opts[:user_nkey_cb]
      @user_credentials ||= opts[:user_credentials]
      @nkeys_seed ||= opts[:nkeys_seed]

      setup_nkeys_connect if @user_credentials or @nkeys_seed

      # Tokens, if set will take preference over the user@server uri token
      @auth_token ||= opts[:auth_token]

      # Check for TLS usage
      @tls = @options[:tls]

      @inbox_prefix = opts.fetch(:custom_inbox_prefix, @inbox_prefix)

      validate_settings!

      self
    end

    private def establish_connection!
      @ruby_pid = Process.pid # For fork detection

      srv = nil
      begin
        srv = select_next_server

        # Create TCP socket connection to NATS
        @io = create_socket
        @io.connect

        # Capture state that we have had a TCP connection established against
        # this server and could potentially be used for reconnecting.
        srv[:was_connected] = true

        # Connection established and now in process of sending CONNECT to NATS
        @status = CONNECTING

        # Use the hostname from the server for TLS hostname verification.
        if client_using_secure_connection? and single_url_connect_used?
          # Always reuse the original hostname used to connect.
          @hostname ||= srv[:hostname]
        else
          @hostname = srv[:hostname]
        end

        # Established TCP connection successfully so can start connect
        process_connect_init

        # Reset reconnection attempts if connection is valid
        srv[:reconnect_attempts] = 0
        srv[:auth_required] ||= true if @server_info[:auth_required]

        # Add back to rotation since successfully connected
        server_pool << srv
      rescue NATS::IO::NoServersError => e
        @disconnect_cb.call(e) if @disconnect_cb
        raise @last_err || e
      rescue => e
        # Capture sticky error
        synchronize do
          @last_err = e
          srv[:auth_required] ||= true if @server_info[:auth_required]
          server_pool << srv if can_reuse_server?(srv)
        end

        err_cb_call(self, e, nil) if @err_cb

        if should_not_reconnect?
          @disconnect_cb.call(e) if @disconnect_cb
          raise e
        end

        # Clean up any connecting state and close connection without
        # triggering the disconnection/closed callbacks.
        close_connection(DISCONNECTED, false)

        # Always sleep here to safe guard against errors before current[:was_connected]
        # is set for the first time.
        sleep @options[:reconnect_time_wait] if @options[:reconnect_time_wait]

        # Continue retrying until there are no options left in the server pool
        retry
      end

      # Initialize queues and loops for message dispatching and processing engine
      @flush_queue = SizedQueue.new(NATS::IO::MAX_FLUSH_KICK_SIZE)
      @pending_queue = SizedQueue.new(NATS::IO::MAX_PENDING_SIZE)
      @pings_outstanding = 0
      @pongs_received = 0
      @pending_size = 0

      # Server roundtrip went ok so consider to be connected at this point
      @status = CONNECTED

      # Connected to NATS so Ready to start parser loop, flusher and ping interval
      start_threads!

      self
    end

    def publish(subject, msg=EMPTY_MSG, opt_reply=nil, **options, &blk)
      raise NATS::IO::BadSubject if !subject or subject.empty?
      if options[:header]
        return publish_msg(NATS::Msg.new(subject: subject, data: msg, reply: opt_reply, header: options[:header]))
      end

      # Accounting
      msg_size = msg.bytesize
      @stats[:out_msgs] += 1
      @stats[:out_bytes] += msg_size

      send_command("PUB #{subject} #{opt_reply} #{msg_size}\r\n#{msg}\r\n")
      @flush_queue << :pub if @flush_queue.empty?
    end

    # Publishes a NATS::Msg that may include headers.
    def publish_msg(msg)
      raise TypeError, "nats: expected NATS::Msg, got #{msg.class.name}" unless msg.is_a?(Msg)
      raise NATS::IO::BadSubject if !msg.subject or msg.subject.empty?

      msg.reply ||= ''
      msg.data ||= ''
      msg_size = msg.data.bytesize

      # Accounting
      @stats[:out_msgs] += 1
      @stats[:out_bytes] += msg_size

      if msg.header
        hdr = ''
        hdr << NATS_HDR_LINE
        msg.header.each do |k, v|
          hdr << "#{k}: #{v}#{CR_LF}"
        end
        hdr << CR_LF
        hdr_len = hdr.bytesize
        total_size = msg_size + hdr_len
        send_command("HPUB #{msg.subject} #{msg.reply} #{hdr_len} #{total_size}\r\n#{hdr}#{msg.data}\r\n")
      else
        send_command("PUB #{msg.subject} #{msg.reply} #{msg_size}\r\n#{msg.data}\r\n")
      end

      @flush_queue << :pub if @flush_queue.empty?
    end

    # Create subscription which is dispatched asynchronously
    # messages to a callback.
    def subscribe(subject, opts={}, &callback)
      raise NATS::IO::ConnectionDrainingError.new("nats: connection draining") if draining?

      sid = nil
      sub = nil
      synchronize do
        sid = (@ssid += 1)
        sub = @subs[sid] = Subscription.new
        sub.nc = self
        sub.sid = sid
      end
      opts[:pending_msgs_limit]  ||= NATS::IO::DEFAULT_SUB_PENDING_MSGS_LIMIT
      opts[:pending_bytes_limit] ||= NATS::IO::DEFAULT_SUB_PENDING_BYTES_LIMIT

      sub.subject = subject
      sub.callback = callback
      sub.received = 0
      sub.queue = opts[:queue] if opts[:queue]
      sub.max = opts[:max] if opts[:max]
      sub.pending_msgs_limit  = opts[:pending_msgs_limit]
      sub.pending_bytes_limit = opts[:pending_bytes_limit]
      sub.pending_queue = SizedQueue.new(sub.pending_msgs_limit)

      send_command("SUB #{subject} #{opts[:queue]} #{sid}#{CR_LF}")
      @flush_queue << :sub

      # Setup server support for auto-unsubscribe when receiving enough messages
      sub.unsubscribe(opts[:max]) if opts[:max]

      unless callback
        cond = sub.new_cond
        sub.wait_for_msgs_cond = cond
      end

      # Async subscriptions each own a single thread for the
      # delivery of messages.
      # FIXME: Support shared thread pool with configurable limits
      # to better support case of having a lot of subscriptions.
      sub.wait_for_msgs_t = Thread.new do
        loop do
          msg = sub.pending_queue.pop

          cb = nil
          sub.synchronize do

            # Decrease pending size since consumed already
            sub.pending_size -= msg.data.size
            cb = sub.callback
          end

          begin
            # Note: Keep some of the alternative arity versions to slightly
            # improve backwards compatibility.  Eventually fine to deprecate
            # since recommended version would be arity of 1 to get a NATS::Msg.
            case cb.arity
            when 0 then cb.call
            when 1 then cb.call(msg)
            when 2 then cb.call(msg.data, msg.reply)
            when 3 then cb.call(msg.data, msg.reply, msg.subject)
            else cb.call(msg.data, msg.reply, msg.subject, msg.header)
            end
          rescue => e
            synchronize do
              err_cb_call(self, e, sub) if @err_cb
            end
          end
        end
      end if callback

      sub
    end

    # Sends a request using expecting a single response using a
    # single subscription per connection for receiving the responses.
    # It times out in case the request is not retrieved within the
    # specified deadline.
    # If given a callback, then the request happens asynchronously.
    def request(subject, payload="", **opts, &blk)
      raise NATS::IO::BadSubject if !subject or subject.empty?

      # If a block was given then fallback to method using auto unsubscribe.
      return old_request(subject, payload, opts, &blk) if blk
      return old_request(subject, payload, opts) if opts[:old_style]

      if opts[:header]
        return request_msg(NATS::Msg.new(subject: subject, data: payload, header: opts[:header]), **opts)
      end

      token = nil
      inbox = nil
      future = nil
      response = nil
      timeout = opts[:timeout] ||= 0.5
      synchronize do
        start_resp_mux_sub! unless @resp_sub_prefix

        # Create token for this request.
        token = @nuid.next
        inbox = "#{@resp_sub_prefix}.#{token}"

        # Create the a future for the request that will
        # get signaled when it receives the request.
        future = @resp_sub.new_cond
        @resp_map[token][:future] = future
      end

      # Publish request and wait for reply.
      publish(subject, payload, inbox)
      begin
        MonotonicTime::with_nats_timeout(timeout) do
          @resp_sub.synchronize do
            future.wait(timeout)
          end
        end
      rescue NATS::Timeout => e
        synchronize { @resp_map.delete(token) }
        raise e
      end

      # Check if there is a response already.
      synchronize do
        result = @resp_map[token]
        response = result[:response]
        @resp_map.delete(token)
      end

      if response and response.header
        status = response.header[STATUS_HDR]
        raise NATS::IO::NoRespondersError if status == "503"
      end

      response
    end

    # request_msg makes a NATS request using a NATS::Msg that may include headers.
    def request_msg(msg, **opts)
      raise TypeError, "nats: expected NATS::Msg, got #{msg.class.name}" unless msg.is_a?(Msg)
      raise NATS::IO::BadSubject if !msg.subject or msg.subject.empty?

      token = nil
      inbox = nil
      future = nil
      response = nil
      timeout = opts[:timeout] ||= 0.5
      synchronize do
        start_resp_mux_sub! unless @resp_sub_prefix

        # Create token for this request.
        token = @nuid.next
        inbox = "#{@resp_sub_prefix}.#{token}"

        # Create the a future for the request that will
        # get signaled when it receives the request.
        future = @resp_sub.new_cond
        @resp_map[token][:future] = future
      end
      msg.reply = inbox
      msg.data ||= ''
      msg_size = msg.data.bytesize

      # Publish request and wait for reply.
      publish_msg(msg)
      begin
        MonotonicTime::with_nats_timeout(timeout) do
          @resp_sub.synchronize do
            future.wait(timeout)
          end
        end
      rescue NATS::Timeout => e
        synchronize { @resp_map.delete(token) }
        raise e
      end

      # Check if there is a response already.
      synchronize do
        result = @resp_map[token]
        response = result[:response]
        @resp_map.delete(token)
      end

      if response and response.header
        status = response.header[STATUS_HDR]
        raise NATS::IO::NoRespondersError if status == "503"
      end

      response
    end

    # Sends a request creating an ephemeral subscription for the request,
    # expecting a single response or raising a timeout in case the request
    # is not retrieved within the specified deadline.
    # If given a callback, then the request happens asynchronously.
    def old_request(subject, payload, opts={}, &blk)
      return unless subject
      inbox = new_inbox

      # If a callback was passed, then have it process
      # the messages asynchronously and return the sid.
      if blk
        opts[:max] ||= 1
        s = subscribe(inbox, opts) do |msg|
          case blk.arity
          when 0 then blk.call
          when 1 then blk.call(msg)
          when 2 then blk.call(msg.data, msg.reply)
          when 3 then blk.call(msg.data, msg.reply, msg.subject)
          else blk.call(msg.data, msg.reply, msg.subject, msg.header)
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
      sub.nc = self

      sid = nil
      synchronize do
        sid = (@ssid += 1)
        sub.sid = sid
        @subs[sid] = sub
      end

      send_command("SUB #{inbox} #{sid}#{CR_LF}")
      @flush_queue << :sub
      unsubscribe(sub, 1)

      sub.synchronize do
        # Publish the request and then wait for the response...
        publish(subject, payload, inbox)

        MonotonicTime::with_nats_timeout(timeout) do
          future.wait(timeout)
        end
      end
      response = sub.response

      if response and response.header
        status = response.header[STATUS_HDR]
        raise NATS::IO::NoRespondersError if status == "503"
      end

      response
    end

    # Send a ping and wait for a pong back within a timeout.
    def flush(timeout=10)
      # Schedule sending a PING, and block until we receive PONG back,
      # or raise a timeout in case the response is past the deadline.
      pong = @pongs.new_cond
      @pongs.synchronize do
        @pongs << pong

        # Flush once pong future has been prepared
        @pending_queue << PING_REQUEST
        @flush_queue << :ping
        MonotonicTime::with_nats_timeout(timeout) do
          pong.wait(timeout)
        end
      end
    end

    alias :servers :server_pool

    # discovered_servers returns the NATS Servers that have been discovered
    # via INFO protocol updates.
    def discovered_servers
      servers.select {|s| s[:discovered] }
    end

    # Close connection to NATS, flushing in case connection is alive
    # and there are any pending messages, should not be used while
    # holding the lock.
    def close
      close_connection(CLOSED, true)
    end

    # new_inbox returns a unique inbox used for subscriptions.
    # @return [String]
    def new_inbox
      "#{@inbox_prefix}.#{@nuid.next}"
    end

    def connected_server
      connected? ? @uri : nil
    end

    def disconnected?
      !@status or @status == DISCONNECTED
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

    def draining?
      if @status == DRAINING_PUBS or @status == DRAINING_SUBS
        return true
      end

      is_draining = false
      synchronize do
        is_draining = true if @drain_t
      end

      is_draining
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

    # drain will put a connection into a drain state. All subscriptions will
    # immediately be put into a drain state. Upon completion, the publishers
    # will be drained and can not publish any additional messages. Upon draining
    # of the publishers, the connection will be closed. Use the `on_close`
    # callback option to know when the connection has moved from draining to closed.
    def drain
      return if draining?

      synchronize do
        @drain_t ||= Thread.new { do_drain }
      end
    end

    # Create a JetStream context.
    # @param opts [Hash] Options to customize the JetStream context.
    # @option params [String] :prefix JetStream API prefix to use for the requests.
    # @option params [String] :domain JetStream Domain to use for the requests.
    # @option params [Float] :timeout Default timeout to use for JS requests.
    # @return [NATS::JetStream]
    def jetstream(opts={})
      ::NATS::JetStream.new(self, opts)
    end
    alias_method :JetStream, :jetstream
    alias_method :jsm, :jetstream

    private

    def validate_settings!
      raise(NATS::IO::ClientError, "custom inbox may not include '>'") if @inbox_prefix.include?(">")
      raise(NATS::IO::ClientError, "custom inbox may not include '*'") if @inbox_prefix.include?("*")
      raise(NATS::IO::ClientError, "custom inbox may not end in '.'") if @inbox_prefix.end_with?(".")
      raise(NATS::IO::ClientError, "custom inbox may not begin with '.'") if @inbox_prefix.start_with?(".")
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
        if !@options[:ignore_discovered_urls] && connect_urls
          srvs = []
          connect_urls.each do |url|
            scheme = client_using_secure_connection? ? "tls" : "nats"
            u = URI.parse("#{scheme}://#{url}")

            # Skip in case it is the current server which we already know
            next if @uri.hostname == u.hostname && @uri.port == u.port

            present = server_pool.detect do |srv|
              srv[:uri].hostname == u.hostname && srv[:uri].port == u.port
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

              # NOTE: Auto discovery won't work here when TLS host verification is enabled.
              srv = { :uri => u, :reconnect_attempts => 0, :discovered => true, :hostname => u.hostname }
              srvs << srv
            end
          end
          srvs.shuffle! unless @options[:dont_randomize_servers]

          # Include in server pool but keep current one as the first one.
          server_pool.push(*srvs)
        end
      end

      @server_info
    end

    def process_hdr(header)
      hdr = nil
      if header
        hdr = {}
        lines = header.lines

        # Check if the first line has an inline status and description.
        if lines.count > 0
          status_hdr = lines.first.rstrip
          status = status_hdr.slice(NATS_HDR_LINE_SIZE-1, STATUS_MSG_LEN)

          if status and !status.empty?
            hdr[STATUS_HDR] = status

            if NATS_HDR_LINE_SIZE+2 < status_hdr.bytesize
              desc = status_hdr.slice(NATS_HDR_LINE_SIZE+STATUS_MSG_LEN, status_hdr.bytesize)
              hdr[DESC_HDR] = desc unless desc.empty?
            end
          end
        end
        begin
          lines.slice(1, header.size).each do |line|
            line.rstrip!
            next if line.empty?
            key, value = line.strip.split(/\s*:\s*/, 2)
            hdr[key] = value
          end
        rescue => e
          err = e
        end
      end

      hdr
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
      # In case of permissions violation then dispatch the error callback
      # while holding the lock.
      e = synchronize do
        current = server_pool.first
        case
        when err =~ /'Stale Connection'/
          @last_err = NATS::IO::StaleConnectionError.new(err)
        when current && current[:auth_required]
          # We cannot recover from auth errors so mark it to avoid
          # retrying to unecessarily next time.
          current[:error_received] = true
          @last_err = NATS::IO::AuthError.new(err)
        else
          @last_err = NATS::IO::ServerError.new(err)
        end
      end
      process_op_error(e)
    end

    def process_msg(subject, sid, reply, data, header)
      @stats[:in_msgs] += 1
      @stats[:in_bytes] += data.size

      # Throw away in case we no longer manage the subscription
      sub = nil
      synchronize { sub = @subs[sid] }
      return unless sub

      err = nil
      sub.synchronize do
        sub.received += 1

        # Check for auto_unsubscribe
        if sub.max
          case
          when sub.received > sub.max
            # Client side support in case server did not receive unsubscribe
            unsubscribe(sid)
            return
          when sub.received == sub.max
            # Cleanup here if we have hit the max..
            synchronize { @subs.delete(sid) }
          end
        end

        # In case of a request which requires a future
        # do so here already while holding the lock and return
        if sub.future
          future = sub.future
          hdr = process_hdr(header)
          sub.response = Msg.new(subject: subject, reply: reply, data: data, header: hdr, nc: self, sub: sub)
          future.signal

          return
        elsif sub.pending_queue
          # Async subscribers use a sized queue for processing
          # and should be able to consume messages in parallel.
          if sub.pending_queue.size >= sub.pending_msgs_limit \
            or sub.pending_size >= sub.pending_bytes_limit then
            err = NATS::IO::SlowConsumer.new("nats: slow consumer, messages dropped")
          else
            hdr = process_hdr(header)

            # Only dispatch message when sure that it would not block
            # the main read loop from the parser.
            msg = Msg.new(subject: subject, reply: reply, data: data, header: hdr, nc: self, sub: sub)
            sub.pending_queue << msg

            # For sync subscribers, signal that there is a new message.
            sub.wait_for_msgs_cond.signal if sub.wait_for_msgs_cond

            sub.pending_size += data.size
          end
        end
      end

      synchronize do
        @last_err = err
        err_cb_call(self, err, sub) if @err_cb
      end if err
    end

    def select_next_server
      raise NATS::IO::NoServersError.new("nats: No servers available") if server_pool.empty?

      # Pick next from head of the list
      srv = server_pool.shift

      # Track connection attempts to this server
      srv[:reconnect_attempts] ||= 0
      srv[:reconnect_attempts] += 1

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

    def single_url_connect_used?
      @single_url_connect_used
    end

    def send_command(command)
      raise NATS::IO::ConnectionClosedError if closed?

      establish_connection! unless status

      @pending_size += command.bytesize
      @pending_queue << command

      # TODO: kick flusher here in case pending_size growing large
    end

    # Auto unsubscribes the server by sending UNSUB command and throws away
    # subscription in case already present and has received enough messages.
    def unsubscribe(sub, opt_max=nil)
      sid = nil
      closed = nil
      sub.synchronize do
        sid = sub.sid
        closed = sub.closed
      end
      raise NATS::IO::BadSubscription.new("nats: invalid subscription") if closed

      opt_max_str = " #{opt_max}" unless opt_max.nil?
      send_command("UNSUB #{sid}#{opt_max_str}#{CR_LF}")
      @flush_queue << :unsub

      synchronize { sub = @subs[sid] }
      return unless sub
      synchronize do
        sub.max = opt_max
        @subs.delete(sid) unless (sub.max && (sub.received < sub.max))

        # Stop messages delivery thread for async subscribers
        if sub.wait_for_msgs_t && sub.wait_for_msgs_t.alive?
          sub.wait_for_msgs_t.exit
          sub.pending_queue.clear
        end
      end

      sub.synchronize do
        sub.closed = true
      end
    end

    def drain_sub(sub)
      sid = nil
      closed = nil
      sub.synchronize do
        sid = sub.sid
        closed = sub.closed
      end
      return if closed

      send_command("UNSUB #{sid}#{CR_LF}")
      @flush_queue << :drain

      synchronize { sub = @subs[sid] }
      return unless sub
    end

    def do_drain
      synchronize { @status = DRAINING_SUBS }

      # Do unsubscribe protocol for all the susbcriptions, then have a single thread
      # waiting until all subs are done or drain timeout error reported to async error cb.
      subs = []
      @subs.each do |_, sub|
        next if sub == @resp_sub
        drain_sub(sub)
        subs << sub
      end
      force_flush!

      # Wait until all subs have no pending messages.
      drain_timeout = MonotonicTime::now + @options[:drain_timeout]
      to_delete = []

      loop do
        break if MonotonicTime::now > drain_timeout
        sleep 0.1

        # Wait until all subs are done.
        @subs.each do |_, sub|
          if sub != @resp_sub and sub.pending_queue.size == 0
            to_delete << sub
          end
        end
        next if to_delete.empty?

        to_delete.each do |sub|
          @subs.delete(sub.sid)
          # Stop messages delivery thread for async subscribers
          if sub.wait_for_msgs_t && sub.wait_for_msgs_t.alive?
            sub.wait_for_msgs_t.exit
            sub.pending_queue.clear
          end
        end
        to_delete.clear

        # Wait until only the resp mux is remaining or there are no subscriptions.
        if @subs.count == 1
          sid, sub = @subs.first
          if sub == @resp_sub
            break
          end
        elsif @subs.count == 0
          break
        end
      end

      if MonotonicTime::now > drain_timeout
        e = NATS::IO::DrainTimeoutError.new("nats: draining connection timed out")
        err_cb_call(self, e, nil) if @err_cb
      end
      synchronize { @status = DRAINING_PUBS }

      # Remove resp mux handler in case there is one.
      unsubscribe(@resp_sub) if @resp_sub
      close
    end

    def send_flush_queue(s)
      @flush_queue << s
    end

    def delete_sid(sid)
      @subs.delete(sid)
    end

    def err_cb_call(nc, e, sub)
      return unless @err_cb

      cb = @err_cb
      case cb.arity
      when 0 then cb.call
      when 1 then cb.call(e)
      when 2 then cb.call(e, sub)
      else cb.call(nc, e, sub)
      end
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

      case
      when auth_connection?
        if @uri.password
          cs[:user] = @uri.user
          cs[:pass] = @uri.password
        else
          cs[:auth_token] = @uri.user
        end
      when @user_jwt_cb && @signature_cb
        nonce = @server_info[:nonce]
        cs[:jwt] = @user_jwt_cb.call
        cs[:sig] = @signature_cb.call(nonce)
      when @user_nkey_cb && @signature_cb
        nonce = @server_info[:nonce]
        cs[:nkey] = @user_nkey_cb.call
        cs[:sig] = @signature_cb.call(nonce)
      end

      cs[:auth_token] = @auth_token if @auth_token

      if @server_info[:headers]
        cs[:headers] = @server_info[:headers]
        cs[:no_responders] = if @options[:no_responders] == false
                               @options[:no_responders]
                             else
                               @server_info[:headers]
                             end
      end

      "CONNECT #{cs.to_json}#{CR_LF}"
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
        err_cb_call(self, e, nil) if @err_cb

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
            rescue NATS::IO::NoServersError => e
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
          data = @io.read(NATS::IO::MAX_SOCKET_READ_BYTES)
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

        force_flush!

        synchronize do
          @pending_size = 0
        end
      end
    end

    def force_flush!
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
          err_cb_call(self, e, nil) if @err_cb
        end

        process_op_error(e)
        return
      end if @io
    end

    def ping_interval_loop
      loop do
        sleep @options[:ping_interval]

        # Skip ping interval until connected
        next if !connected?

        if @pings_outstanding >= @options[:max_outstanding_pings]
          process_op_error(NATS::IO::StaleConnectionError.new("nats: stale connection"))
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
      if !line or line.empty?
        raise NATS::IO::ConnectError.new("nats: protocol exception, INFO not received")
      end

      if match = line.match(NATS::Protocol::INFO)
        info_json = match.captures.first
        process_info(info_json)
      else
        raise NATS::IO::ConnectError.new("nats: protocol exception, INFO not valid")
      end

      case
      when (server_using_secure_connection? and client_using_secure_connection?)
        tls_context = nil

        if @tls
          # Allow prepared context and customizations via :tls opts
          tls_context = @tls[:context] if @tls[:context]
        else
          # Defaults
          tls_context = OpenSSL::SSL::SSLContext.new

          # Use the default verification options from Ruby:
          # https://github.com/ruby/ruby/blob/96db72ce38b27799dd8e80ca00696e41234db6ba/ext/openssl/lib/openssl/ssl.rb#L19-L29
          #
          # Insecure TLS versions not supported already:
          # https://github.com/ruby/openssl/commit/3e5a009966bd7f806f7180d82cf830a04be28986
          #
          tls_context.set_params
        end

        # Setup TLS connection by rewrapping the socket
        tls_socket = OpenSSL::SSL::SSLSocket.new(@io.socket, tls_context)

        # Close TCP socket after closing TLS socket as well.
        tls_socket.sync_close = true

        # Required to enable hostname verification if Ruby runtime supports it (>= 2.4):
        # https://github.com/ruby/openssl/commit/028e495734e9e6aa5dba1a2e130b08f66cf31a21
        tls_socket.hostname = @hostname

        tls_socket.connect
        @io.socket = tls_socket
      when (server_using_secure_connection? and !client_using_secure_connection?)
        raise NATS::IO::ConnectError.new('TLS/SSL required by server')
      when (client_using_secure_connection? and !server_using_secure_connection?)
        raise NATS::IO::ConnectError.new('TLS/SSL not supported by server')
      else
        # Otherwise, use a regular connection.
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
      srv = nil
      begin
        srv = select_next_server

        # Establish TCP connection with new server
        @io = create_socket
        @io.connect
        @stats[:reconnects] += 1

        # Set hostname to use for TLS hostname verification
        if client_using_secure_connection? and single_url_connect_used?
          # Reuse original hostname name in case of using TLS.
          @hostname ||= srv[:hostname]
        else
          @hostname = srv[:hostname]
        end

        # Established TCP connection successfully so can start connect
        process_connect_init

        # Reset reconnection attempts if connection is valid
        srv[:reconnect_attempts] = 0
        srv[:auth_required] ||= true if @server_info[:auth_required]

        # Add back to rotation since successfully connected
        server_pool << srv
      rescue NATS::IO::NoServersError => e
        raise e
      rescue => e
        # In case there was an error from the server check
        # to see whether need to take it out from rotation
        srv[:auth_required] ||= true if @server_info[:auth_required]
        server_pool << srv if can_reuse_server?(srv)

        @last_err = e

        # Trigger async error handler
        err_cb_call(self, e, nil) if @err_cb

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

      # Reset parser state here to avoid unknown protocol errors
      # on reconnect...
      @parser.reset!

      # Now connected to NATS, and we can restart parser loop, flusher
      # and ping interval
      start_threads!

      # Dispatch the reconnected callback while holding lock
      # which we should have already
      @reconnect_cb.call if @reconnect_cb
    end

    def close_connection(conn_status, do_cbs=true)
      synchronize do
        @connect_called = false
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
          err_cb_call(self, e, nil) if @err_cb
        end if should_flush

        # Destroy any remaining subscriptions.
        @subs.each do |_, sub|
          if sub.wait_for_msgs_t && sub.wait_for_msgs_t.alive?
            sub.wait_for_msgs_t.exit
            sub.pending_queue.clear
          end
        end
        @subs.clear

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

    # Prepares requests subscription that handles the responses
    # for the new style request response.
    def start_resp_mux_sub!
      @resp_sub_prefix = new_inbox
      @resp_map = Hash.new { |h,k| h[k] = { }}

      @resp_sub = Subscription.new
      @resp_sub.subject = "#{@resp_sub_prefix}.*"
      @resp_sub.received = 0
      @resp_sub.nc = self

      # FIXME: Allow setting pending limits for responses mux subscription.
      @resp_sub.pending_msgs_limit = NATS::IO::DEFAULT_SUB_PENDING_MSGS_LIMIT
      @resp_sub.pending_bytes_limit = NATS::IO::DEFAULT_SUB_PENDING_BYTES_LIMIT
      @resp_sub.pending_queue = SizedQueue.new(@resp_sub.pending_msgs_limit)
      @resp_sub.wait_for_msgs_t = Thread.new do
        loop do
          msg = @resp_sub.pending_queue.pop
          @resp_sub.pending_size -= msg.data.size

          # Pick the token and signal the request under the mutex
          # from the subscription itself.
          token = msg.subject.split('.').last
          future = nil
          synchronize do
            future = @resp_map[token][:future]
            @resp_map[token][:response] = msg
          end

          # Signal back that the response has arrived
          # in case the future has not been yet delete.
          @resp_sub.synchronize do
            future.signal if future
          end
        end
      end

      sid = (@ssid += 1)
      @resp_sub.sid = sid
      @subs[sid] = @resp_sub
      send_command("SUB #{@resp_sub.subject} #{sid}#{CR_LF}")
      @flush_queue << :sub
    end

    def can_reuse_server?(server)
      return false if server.nil?

      # We can always reuse servers with infinite reconnects settings
      return true if @options[:max_reconnect_attempts] < 0

      # In case of hard errors like authorization errors, drop the server
      # already since won't be able to connect.
      return false if server[:error_received]

      # We will retry a number of times to reconnect to a server.
      return server[:reconnect_attempts] <= @options[:max_reconnect_attempts]
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
          connect_timeout: NATS::IO::DEFAULT_CONNECT_TIMEOUT
        })
    end

    def setup_nkeys_connect
      begin
        require 'nkeys'
        require 'base64'
      rescue LoadError
        raise(Error, "nkeys is not installed")
      end

      case
      when @nkeys_seed
        @user_nkey_cb = nkey_cb_for_nkey_file(@nkeys_seed)
        @signature_cb = signature_cb_for_nkey_file(@nkeys_seed)
      when @user_credentials
        # When the credentials are within a single decorated file.
        @user_jwt_cb = jwt_cb_for_creds_file(@user_credentials)
        @signature_cb = signature_cb_for_creds_file(@user_credentials)
      end
    end

    def signature_cb_for_nkey_file(nkey)
      proc { |nonce|
        seed = File.read(nkey).chomp
        kp = NKEYS::from_seed(seed)
        raw_signed = kp.sign(nonce)
        kp.wipe!
        encoded = Base64.urlsafe_encode64(raw_signed)
        encoded.gsub('=', '')
      }
    end

    def nkey_cb_for_nkey_file(nkey)
      proc {
        seed = File.read(nkey).chomp
        kp = NKEYS::from_seed(seed)

        # Take a copy since original will be gone with the wipe.
        pub_key = kp.public_key.dup
        kp.wipe!

        pub_key
      }
    end

    def jwt_cb_for_creds_file(creds)
      proc {
        jwt_start = "BEGIN NATS USER JWT".freeze
        found = false
        jwt = nil

        File.readlines(creds).each do |line|
          case
          when found
            jwt = line.chomp
            break
          when line.include?(jwt_start)
            found = true
          end
        end

        raise(Error, "No JWT found in #{creds}") if not found

        jwt
      }
    end

    def signature_cb_for_creds_file(creds)
      proc { |nonce|
        seed_start = "BEGIN USER NKEY SEED".freeze
        found = false
        seed = nil

        File.readlines(creds).each do |line|
          case
          when found
            seed = line.chomp
            break
          when line.include?(seed_start)
            found = true
          end
        end

        raise(Error, "No nkey user seed found in #{creds}") if not found

        kp = NKEYS::from_seed(seed)
        raw_signed = kp.sign(nonce)

        # seed is a reference so also cleared when doing wipe,
        # which can be done since Ruby strings are mutable.
        kp.wipe
        encoded = Base64.urlsafe_encode64(raw_signed)

        # Remove padding
        encoded.gsub('=', '')
      }
    end

    def process_uri(uris)
      uris.split(',').map do |uri|
        opts = {}

        # Scheme
        uri = "nats://#{uri}" if !uri.include?("://")

        uri_object = URI(uri)

        # Host and Port
        uri_object.hostname ||= "localhost"
        uri_object.port ||= DEFAULT_PORT

        uri_object
      end
    end
  end

  module IO
    include Status

    # Client creates a connection to the NATS Server.
    Client = ::NATS::Client

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
    DEFAULT_DRAIN_TIMEOUT = 30

    # Default Pending Limits
    DEFAULT_SUB_PENDING_MSGS_LIMIT  = 65536
    DEFAULT_SUB_PENDING_BYTES_LIMIT = 65536 * 1024

    # Implementation adapted from https://github.com/redis/redis-rb
    class Socket
      attr_accessor :socket

      def initialize(options={})
        @uri = options[:uri]
        @connect_timeout = options[:connect_timeout]
        @write_timeout = options[:write_timeout]
        @read_timeout = options[:read_timeout]
        @socket = nil
      end

      def connect
        addrinfo = ::Socket.getaddrinfo(@uri.hostname, nil, ::Socket::AF_UNSPEC, ::Socket::SOCK_STREAM)
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
          raise NATS::IO::SocketTimeoutError
        end
        @socket.gets
      end

      def read(max_bytes, deadline=nil)

        begin
          return @socket.read_nonblock(max_bytes)
        rescue ::IO::WaitReadable
          if ::IO.select([@socket], nil, nil, deadline)
            retry
          else
            raise NATS::IO::SocketTimeoutError
          end
        rescue ::IO::WaitWritable
          if ::IO.select(nil, [@socket], nil, deadline)
            retry
          else
            raise NATS::IO::SocketTimeoutError
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
          rescue ::IO::WaitWritable
            if ::IO.select(nil, [@socket], nil, deadline)
              retry
            else
              raise NATS::IO::SocketTimeoutError
            end
          rescue ::IO::WaitReadable
            if ::IO.select([@socket], nil, nil, deadline)
              retry
            else
              raise NATS::IO::SocketTimeoutError
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
            raise NATS::IO::SocketTimeoutError
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

  NANOSECONDS = 1_000_000_000

  class MonotonicTime
    # Implementation of MonotonicTime adapted from
    # https://github.com/ruby-concurrency/concurrent-ruby/
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

      def with_nats_timeout(timeout)
        start_time = now
        yield
        end_time = now
        duration = end_time - start_time
        if duration > timeout
          raise NATS::Timeout.new("nats: timeout")
        end
      end

      def since(t0)
        now - t0
      end
    end
  end
end
