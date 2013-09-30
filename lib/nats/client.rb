require 'uri'
require 'securerandom'

ep = File.expand_path(File.dirname(__FILE__))

require "#{ep}/ext/em"
require "#{ep}/ext/bytesize"
require "#{ep}/ext/json"

module NATS

  VERSION = "0.5.0.beta.11".freeze

  DEFAULT_PORT = 4222
  DEFAULT_URI = "nats://localhost:#{DEFAULT_PORT}".freeze

  MAX_RECONNECT_ATTEMPTS = 10
  RECONNECT_TIME_WAIT = 2

  MAX_PENDING_SIZE = 32768

  # Maximum outbound size per client to trigger FP, 20MB
  FAST_PRODUCER_THRESHOLD = (10*1024*1024)

  # Ping intervals
  DEFAULT_PING_INTERVAL = 120
  DEFAULT_PING_MAX = 2

  # Protocol
  # @private
  MSG      = /\AMSG\s+([^\s]+)\s+([^\s]+)\s+(([^\s]+)[^\S\r\n]+)?(\d+)\r\n/i #:nodoc:
  OK       = /\A\+OK\s*\r\n/i #:nodoc:
  AUTH_OK  = /\A\+AUTH_OK\s*\r\n/i #:nodoc:
  ERR      = /\A-ERR\s+('.+')?\r\n/i #:nodoc:
  PING     = /\APING\s*\r\n/i #:nodoc:
  PONG     = /\APONG\s*\r\n/i #:nodoc:
  INFO     = /\AINFO\s+([^\r\n]+)\r\n/i  #:nodoc:
  UNKNOWN  = /\A(.*)\r\n/  #:nodoc:

  # Responses
  CR_LF = ("\r\n".freeze) #:nodoc:
  CR_LF_SIZE = (CR_LF.bytesize) #:nodoc:

  PING_REQUEST  = ("PING#{CR_LF}".freeze) #:nodoc:
  PONG_RESPONSE = ("PONG#{CR_LF}".freeze) #:nodoc:

  EMPTY_MSG = (''.freeze) #:nodoc:

  # Used for future pedantic Mode
  SUB = /^([^\.\*>\s]+|>$|\*)(\.([^\.\*>\s]+|>$|\*))*$/ #:nodoc:
  SUB_NO_WC = /^([^\.\*>\s]+)(\.([^\.\*>\s]+))*$/ #:nodoc:

  # Parser
  AWAITING_CONTROL_LINE = 1 #:nodoc:
  AWAITING_MSG_PAYLOAD  = 2 #:nodoc:

  # Autostart properties
  AUTOSTART_PID_FILE = '/tmp/nats-server.pid'
  AUTOSTART_LOG_FILE = '/tmp/nats-server.log'

  # Duplicate autostart protection
  @@tried_autostart = {}

  class Error < StandardError; end #:nodoc:

  # When the NATS server sends us an ERROR message, this is raised/passed by default
  class ServerError < Error; end #:nodoc:

  # When we detect error on the client side (e.g. Fast Producer)
  class ClientError < Error; end #:nodoc:

  # When we cannot connect to the server (either initially or after a reconnect), this is raised/passed
  class ConnectError < Error; end #:nodoc:

  class << self
    attr_reader   :client, :reactor_was_running, :err_cb, :err_cb_overridden #:nodoc:
    attr_reader   :reconnect_cb  #:nodoc
    attr_accessor :timeout_cb #:nodoc

    alias :reactor_was_running? :reactor_was_running

    # Create and return a connection to the server with the given options.
    # The server will be autostarted if requested and the <b>uri</b> is determined to be local.
    # The optional block will be called when the connection has been completed.
    #
    # @param [Hash] opts
    # @option opts [String|URI] :uri The URI to connect to, example nats://localhost:4222
    # @option opts [Boolean] :autostart Boolean that can be used to engage server autostart functionality.
    # @option opts [Boolean] :reconnect Boolean that can be used to suppress reconnect functionality.
    # @option opts [Boolean] :debug Boolean that can be used to output additional debug information.
    # @option opts [Boolean] :verbose Boolean that is sent to server for setting verbose protocol mode.
    # @option opts [Boolean] :pedantic Boolean that is sent to server for setting pedantic mode.
    # @option opts [Boolean] :ssl Boolean that is sent to server for setting TLS/SSL mode.
    # @option opts [Integer] :max_reconnect_attempts Integer that can be used to set the max number of reconnect tries
    # @option opts [Integer] :reconnect_time_wait Integer that can be used to set the number of seconds to wait between reconnect tries
    # @option opts [Integer] :ping_interval Integer that can be used to set the ping interval in seconds.
    # @option opts [Integer] :max_outstanding_pings Integer that can be used to set the max number of outstanding pings before declaring a connection closed.
    # @param [Block] &blk called when the connection is completed. Connection will be passed to the block.
    # @return [NATS] connection to the server.
    def connect(opts={}, &blk)
      # Defaults
      opts[:verbose] = false if opts[:verbose].nil?
      opts[:pedantic] = false if opts[:pedantic].nil?
      opts[:reconnect] = true if opts[:reconnect].nil?
      opts[:ssl] = false if opts[:ssl].nil?
      opts[:max_reconnect_attempts] = MAX_RECONNECT_ATTEMPTS if opts[:max_reconnect_attempts].nil?
      opts[:reconnect_time_wait] = RECONNECT_TIME_WAIT if opts[:reconnect_time_wait].nil?
      opts[:ping_interval] = DEFAULT_PING_INTERVAL if opts[:ping_interval].nil?
      opts[:max_outstanding_pings] = DEFAULT_PING_MAX if opts[:max_outstanding_pings].nil?

      # Override with ENV
      opts[:uri] ||= ENV['NATS_URI'] || DEFAULT_URI
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

      uri = opts[:uris] || opts[:servers] || opts[:uri]

      # If they pass an array here just pass along to the real connection, and use first as the first attempt..
      # Real connection will do proper walk throughs etc..
      unless uri.nil?
        u = uri.kind_of?(Array) ? uri.first : uri
        @uri = u.is_a?(URI) ? u.dup : URI.parse(u)
      end

      @err_cb = proc { |e| raise e } unless err_cb
      check_autostart(@uri) if opts[:autostart] == true

      client = EM.connect(@uri.host, @uri.port, self, opts)
      client.on_connect(&blk) if blk
      return client
    end

    # Create a default client connection to the server.
    # @see NATS::connect
    def start(*args, &blk)
      @reactor_was_running = EM.reactor_running?
      unless (@reactor_was_running || blk)
        raise(Error, "EM needs to be running when NATS.start is called without a run block")
      end
      # Setup optimized select versions
      EM.epoll; EM.kqueue
      EM.run { @client = connect(*args, &blk) }
    end

    # Close the default client connection and optionally call the associated block.
    # @param [Block] &blk called when the connection is closed.
    def stop(&blk)
      client.close if (client and (client.connected? || client.reconnecting?))
      blk.call if blk
      @@tried_autostart = {}
      @err_cb = nil
    end

    # @return [Boolean] Connected state
    def connected?
      return false unless client
      client.connected?
    end

    # @return [Boolean] Reconnecting state
    def reconnecting?
      return false unless client
      client.reconnecting?
    end

    # @return [Hash] Options
    def options
      return {} unless client
      client.options
    end

    # @return [Hash] Server information
    def server_info
      return nil unless client
      client.server_info
    end

    # Set the default on_error callback.
    # @param [Block] &callback called when an error has been detected.
    def on_error(&callback)
      @err_cb, @err_cb_overridden = callback, true
    end

    # Set the default on_reconnect callback.
    # @param [Block] &callback called when a reconnect attempt is made.
    def on_reconnect(&callback)
      @reconnect_cb = callback
      @client.on_reconnect(&callback) unless @client.nil?
    end

    # Publish a message using the default client connection.
    # @see NATS#publish
    def publish(*args, &blk)
      (@client ||= connect).publish(*args, &blk)
    end

    # Subscribe using the default client connection.
    # @see NATS#subscribe
    def subscribe(*args, &blk)
      (@client ||= connect).subscribe(*args, &blk)
    end

    # Cancel a subscription on the default client connection.
    # @see NATS#unsubscribe
    def unsubscribe(*args)
      (@client ||= connect).unsubscribe(*args)
    end

    # Set a timeout for receiving messages for the subscription.
    # @see NATS#timeout
    def timeout(*args, &blk)
      (@client ||= connect).timeout(*args, &blk)
    end

    # Publish a message and wait for a response on the default client connection.
    # @see NATS#request
    def request(*args, &blk)
      (@client ||= connect).request(*args, &blk)
    end

    # Returns a subject that can be used for "directed" communications.
    # @return [String]
    def create_inbox
      "_INBOX.#{SecureRandom.hex(13)}"
    end

    # Flushes all messages and subscriptions in the default connection
    # @see NATS#flush
    def flush(*args, &blk)
      (@client ||= connect).flush(*args, &blk)
    end

    # Return bytes outstanding for the default client connection.
    # @see NATS#pending_data_size
    def pending_data_size(*args)
      (@client ||= connect).pending_data_size(*args)
    end

    def wait_for_server(uri, max_wait = 5) # :nodoc:
      start = Time.now
      while (Time.now - start < max_wait) # Wait max_wait seconds max
        break if server_running?(uri)
        sleep(0.1)
      end
    end

    def server_running?(uri) # :nodoc:
      require 'socket'
      s = TCPSocket.new(uri.host, uri.port)
      s.close
      return true
    rescue
      return false
    end

    def clear_client # :nodoc:
      @client = nil
    end

    private

    def check_autostart(uri)
      return if uri_is_remote?(uri) || @@tried_autostart[uri]
      @@tried_autostart[uri] = true
      return if server_running?(uri)
      return unless try_autostart_succeeded?(uri)
      wait_for_server(uri)
    end

    def uri_is_remote?(uri)
      uri.host != 'localhost' && uri.host != '127.0.0.1'
    end

    def try_autostart_succeeded?(uri)
      port_arg = "-p #{uri.port}"
      user_arg = "--user #{uri.user}" if uri.user
      pass_arg = "--pass #{uri.password}" if uri.password
      log_arg  = "-l #{AUTOSTART_LOG_FILE}"
      pid_arg  = "-P #{AUTOSTART_PID_FILE}"
      # daemon mode to release client
      `nats-server #{port_arg} #{user_arg} #{pass_arg} #{log_arg} #{pid_arg} -d 2> /dev/null`
      $? == 0
    end

  end

  attr_reader :connected, :connect_cb, :err_cb, :err_cb_overridden, :pongs_received #:nodoc:
  attr_reader :closing, :reconnecting, :server_pool, :options, :server_info #:nodoc
  attr_reader :msgs_received, :msgs_sent, :bytes_received, :bytes_sent, :pings

  alias :connected? :connected
  alias :closing? :closing
  alias :reconnecting? :reconnecting

  def initialize(options)
    @options = options
    process_uri_options
    @ssl = options[:ssl] if options[:ssl]
    @ssid, @subs = 1, {}
    @err_cb = NATS.err_cb
    @reconnect_timer, @needed = nil, nil
    @reconnect_cb = NATS.reconnect_cb
    @connected, @closing, @reconnecting = false, false, false
    @msgs_received = @msgs_sent = @bytes_received = @bytes_sent = @pings = 0
    @pending_size = 0
    send_connect_command
  end

  # Publish a message to a given subject, with optional reply subject and completion block
  # @param [String] subject
  # @param [Object, #to_s] msg
  # @param [String] opt_reply
  # @param [Block] blk, closure called when publish has been processed by the server.
  def publish(subject, msg=EMPTY_MSG, opt_reply=nil, &blk)
    return unless subject
    msg = msg.to_s

    # Accounting
    @msgs_sent += 1
    @bytes_sent += msg.bytesize if msg

    send_command("PUB #{subject} #{opt_reply} #{msg.bytesize}#{CR_LF}#{msg}#{CR_LF}")
    queue_server_rt(&blk) if blk
  end

  # Subscribe to a subject with optional wildcards.
  # Messages will be delivered to the supplied callback.
  # Callback can take any number of the supplied arguments as defined by the list: msg, reply, sub.
  # Returns subscription id which can be passed to #unsubscribe.
  # @param [String] subject, optionally with wilcards.
  # @param [Hash] opts, optional options hash, e.g. :queue, :max.
  # @param [Block] callback, called when a message is delivered.
  # @return [Object] sid, Subject Identifier
  def subscribe(subject, opts={}, &callback)
    return unless subject
    sid = (@ssid += 1)
    sub = @subs[sid] = { :subject => subject, :callback => callback, :received => 0 }
    sub[:queue] = opts[:queue] if opts[:queue]
    sub[:max] = opts[:max] if opts[:max]
    send_command("SUB #{subject} #{opts[:queue]} #{sid}#{CR_LF}")
    # Setup server support for auto-unsubscribe
    unsubscribe(sid, opts[:max]) if opts[:max]
    sid
  end

  # Cancel a subscription.
  # @param [Object] sid
  # @param [Number] opt_max, optional number of responses to receive before auto-unsubscribing
  def unsubscribe(sid, opt_max=nil)
    opt_max_str = " #{opt_max}" unless opt_max.nil?
    send_command("UNSUB #{sid}#{opt_max_str}#{CR_LF}")
    return unless sub = @subs[sid]
    sub[:max] = opt_max
    @subs.delete(sid) unless (sub[:max] && (sub[:received] < sub[:max]))
  end

  # Return the active subscription count.
  # @return [Number]
  def subscription_count
    @subs.size
  end

  # Setup a timeout for receiving messages for the subscription.
  # @param [Object] sid
  # @param [Number] timeout, float in seconds
  # @param [Hash] opts, options, :auto_unsubscribe(true), :expected(1)
  def timeout(sid, timeout, opts={}, &callback)
    # Setup a timeout if requested
    return unless sub = @subs[sid]

    auto_unsubscribe, expected = true, 1
    auto_unsubscribe = opts[:auto_unsubscribe] if opts.key?(:auto_unsubscribe)
    expected = opts[:expected] if opts.key?(:expected)

    EM.cancel_timer(sub[:timeout]) if sub[:timeout]

    sub[:timeout] = EM.add_timer(timeout) do
      unsubscribe(sid) if auto_unsubscribe
      callback.call(sid) if callback
    end
    sub[:expected] = expected
  end

  # Send a request and have the response delivered to the supplied callback.
  # @param [String] subject
  # @param [Object] msg
  # @param [Block] callback
  # @return [Object] sid
  def request(subject, data=nil, opts={}, &cb)
    return unless subject
    inbox = NATS.create_inbox
    s = subscribe(inbox, opts) { |msg, reply|
      case cb.arity
        when 0 then cb.call
        when 1 then cb.call(msg)
        else cb.call(msg, reply)
      end
    }
    publish(subject, data, inbox)
    return s
  end

  # Flushes all messages and subscriptions for the connection.
  # All messages and subscriptions have been processed by the server
  # when the optional callback is called.
  def flush(&blk)
    queue_server_rt(&blk) if blk
  end

  # Define a callback to be called when the client connection has been established.
  # @param [Block] callback
  def on_connect(&callback)
    @connect_cb = callback
  end

  # Define a callback to be called when errors occur on the client connection.
  # @param [Block] &blk called when the connection is closed.
  def on_error(&callback)
    @err_cb, @err_cb_overridden = callback, true
  end

  # Define a callback to be called when a reconnect attempt is made.
  # @param [Block] &blk called when the connection is closed.
  def on_reconnect(&callback)
    @reconnect_cb = callback
  end

  # Close the connection to the server.
  def close
    @closing = true
    cancel_ping_timer
    cancel_reconnect_timer
    close_connection_after_writing if connected?
    process_disconnect if reconnecting?
  end

  # Return bytes outstanding waiting to be sent to server.
  def pending_data_size
    get_outbound_data_size + @pending_size
  end

  def user_err_cb? # :nodoc:
    err_cb_overridden || NATS.err_cb_overridden
  end

  def connect_command #:nodoc:
    cs = { :verbose => @options[:verbose], :pedantic => @options[:pedantic] }
    if @uri.user
      cs[:user] = @uri.user
      cs[:pass] = @uri.password
    end
    cs[:ssl_required] = @ssl if @ssl
    "CONNECT #{cs.to_json}#{CR_LF}"
  end

  def send_connect_command #:nodoc:
    send_command(connect_command, true)
  end

  def queue_server_rt(&cb) #:nodoc:
    return unless cb
    (@pongs ||= []) << cb
    send_command(PING_REQUEST)
  end

  def on_msg(subject, sid, reply, msg) #:nodoc:

    # Accounting - We should account for inbound even if they are not processed.
    @msgs_received += 1
    @bytes_received += msg.bytesize if msg

    return unless sub = @subs[sid]

    # Check for auto_unsubscribe
    sub[:received] += 1
    if sub[:max]
      # Client side support in case server did not receive unsubscribe
      return unsubscribe(sid) if (sub[:received] > sub[:max])
      # cleanup here if we have hit the max..
      @subs.delete(sid) if (sub[:received] == sub[:max])
    end

    if cb = sub[:callback]
      case cb.arity
        when 0 then cb.call
        when 1 then cb.call(msg)
        when 2 then cb.call(msg, reply)
        else cb.call(msg, reply, subject)
      end
    end

    # Check for a timeout, and cancel if received >= expected
    if (sub[:timeout] && sub[:received] >= sub[:expected])
      EM.cancel_timer(sub[:timeout])
      sub[:timeout] = nil
    end
  end

  def flush_pending #:nodoc:
    return unless @pending
    send_data(@pending.join)
    @pending, @pending_size = nil, 0
  end

  def receive_data(data) #:nodoc:
    @buf = @buf ? @buf << data : data
    while (@buf)
      case @parse_state
      when AWAITING_CONTROL_LINE
        case @buf
        when MSG
          @buf = $'
          @sub, @sid, @reply, @needed = $1, $2.to_i, $4, $5.to_i
          @parse_state = AWAITING_MSG_PAYLOAD
        when OK # No-op right now
          @buf = $'
        when AUTH_OK # Start ping timer after auth is ok
          @buf = $'
          add_ping_timer
        when ERR
          @buf = $'
          err_cb.call(NATS::ServerError.new($1))
        when PING
          @pings += 1
          @buf = $'
          send_command(PONG_RESPONSE)
        when PONG
          @buf = $'
          cb = @pongs.shift
          cb.call if cb
        when INFO
          @buf = $'
          process_info($1)
        when UNKNOWN
          @buf = $'
          err_cb.call(NATS::ServerError.new("Unknown protocol: #{$1}"))
        else
          # If we are here we do not have a complete line yet that we understand.
          return
        end
        @buf = nil if (@buf && @buf.empty?)

      when AWAITING_MSG_PAYLOAD
        return unless (@needed && @buf.bytesize >= (@needed + CR_LF_SIZE))
        on_msg(@sub, @sid, @reply, @buf.slice(0, @needed))
        @buf = @buf.slice((@needed + CR_LF_SIZE), @buf.bytesize)
        @sub = @sid = @reply = @needed = nil
        @parse_state = AWAITING_CONTROL_LINE
        @buf = nil if (@buf && @buf.empty?)
      end

    end
  end

  def process_info(info) #:nodoc:
    @server_info = JSON.parse(info, :symbolize_keys => true, :symbolize_names => true)
    if @server_info[:ssl_required] && @ssl
      start_tls
    else
      if @server_info[:ssl_required]
        err_cb.call(NATS::ClientError.new('TLS/SSL required by server'))
      elsif @ssl
        err_cb.call(NATS::ClientError.new('TLS/SSL not supported by server'))
      end
    end
    @server_info
  end

  def ssl_handshake_completed
    @connected = true
    flush_pending
  end

  def add_ping_timer
     @pings_outstanding = 0
     @pongs_received = 0
     @ping_timer = EM.add_periodic_timer(@options[:ping_interval]) { send_ping } 
  end

  def cancel_ping_timer
    if @ping_timer
      EM.cancel_timer(@ping_timer)
      @ping_timer = nil
    end
  end

  def connection_completed #:nodoc:
    @connected = true unless @ssl

    current = server_pool.first
    current[:was_connected] = true
    current[:reconnect_attempts] ||= 0

    if reconnecting?
      cancel_reconnect_timer
      @subs.each_pair { |k, v| send_command("SUB #{v[:subject]} #{v[:queue]} #{k}#{CR_LF}") }
    end

    flush_pending unless @ssl

    unless user_err_cb? or reconnecting?
      @err_cb = proc { |e| raise e }
    end

    if (connect_cb and not reconnecting?)
      # We will round trip the server here to make sure all state from any pending commands
      # has been processed before calling the connect callback.
      queue_server_rt { connect_cb.call(self) }
    end
    @reconnecting = false
    @parse_state = AWAITING_CONTROL_LINE

  end

  def send_ping #:nodoc:
    return if @closing
    if @pings_outstanding > @options[:max_outstanding_pings]
      close_connection
      #close
      return
    end
    @pings_outstanding += 1
    queue_server_rt { process_pong }
    flush_pending
  end

  def process_pong
    @pongs_received += 1
    @pings_outstanding -= 1
  end

  def should_delay_connect?(server)
    server[:was_connected] && server[:reconnect_attempts] >= 1
  end

  def schedule_reconnect #:nodoc:
    @reconnecting = true
    @connected = false
    @reconnect_timer = EM.add_timer(@options[:reconnect_time_wait]) { attempt_reconnect }
  end

  def unbind #:nodoc:
    # If we are closing or shouldn't reconnect, go ahead and disconnect.
    process_disconnect and return if (closing? or should_not_reconnect?)

    @reconnecting = true if connected?
    @connected = false
    @pending = @pongs = nil

    schedule_primary_and_connect
  end

  def multiple_servers_available?
    server_pool && server_pool.size > 1
  end

  def should_not_reconnect?
    !@options[:reconnect]
  end

  def cancel_reconnect_timer
    if @reconnect_timer
      EM.cancel_timer(@reconnect_timer)
      @reconnect_timer = nil
    end
  end

  def disconnect_error_string
    return "Client disconnected from server on #{@uri}." if @connected
    return "Could not connect to server on #{@uri}"
  end

  def process_disconnect #:nodoc:
    err_cb.call(NATS::ConnectError.new(disconnect_error_string)) if not closing? and @err_cb
    true # Chaining
  ensure
    cancel_ping_timer
    cancel_reconnect_timer
    if (NATS.client == self)
      NATS.clear_client
      EM.stop if ((connected? || reconnecting?) and closing? and not NATS.reactor_was_running?)
    end
    @connected = @reconnecting = false
  end

  def can_reuse_server?(server) #:nodoc:
    reconnecting? && server[:was_connected] && server[:reconnect_attempts] <= @options[:max_reconnect_attempts]
  end

  def attempt_reconnect #:nodoc:
    @reconnect_timer = nil
    current = server_pool.first
    current[:reconnect_attempts] += 1 if current[:reconnect_attempts]
    send_connect_command
    EM.reconnect(@uri.host, @uri.port, self)
    @reconnect_cb.call unless @reconnect_cb.nil?
  end

  def send_command(command, priority = false) #:nodoc:
    EM.next_tick { flush_pending } if (connected? && @pending.nil?)
    @pending ||= []
    @pending << command unless priority
    @pending.unshift(command) if priority
    @pending_size += command.bytesize
    flush_pending if (connected? && @pending_size > MAX_PENDING_SIZE)
    if (@options[:fast_producer_error] && pending_data_size > FAST_PRODUCER_THRESHOLD)
      err_cb.call(NATS::ClientError.new("Fast Producer: #{pending_data_size} bytes outstanding"))
    end
    true
  end

  # Parse out URIs which can now be an array of server choices
  # The server pool will contain both explicit and implicit members.
  def process_uri_options #:nodoc
    @server_pool = []
    uri = options[:uris] || options[:servers] || options[:uri]
    uri = uri.kind_of?(Array) ? uri : [uri]
    uri.each { |u| server_pool << { :uri => u.is_a?(URI) ? u.dup : URI.parse(u) } }
    server_pool.shuffle! unless options[:dont_randomize_servers]
    bind_primary
  end

  def connected_server
    connected? ? @uri : nil
  end

  def bind_primary #:nodoc:
    first = server_pool.first
    @uri = first[:uri]
    @uri.user = options[:user] if options[:user]
    @uri.password = options[:pass] if options[:pass]
    first
  end

  # We have failed on an attempt at the primary (first) server, rotate and try again
  def schedule_primary_and_connect #:nodoc:
    # Dump the one we were trying if it wasn't connected
    current = server_pool.shift
    server_pool << current if can_reuse_server?(current)

    # If we are out of options, go ahead and disconnect.
    process_disconnect and return if server_pool.empty?
    # bind new one
    next_server = bind_primary
    # If the next one was connected and we are trying to reconnect
    # set up timer if we tried once already.
    if should_delay_connect?(next_server)
      schedule_reconnect
    else
      attempt_reconnect
    end
  end

  def inspect #:nodoc:
    "<nats client v#{NATS::VERSION}>"
  end

end
