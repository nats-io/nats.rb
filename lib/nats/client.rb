require 'uri'

ep = File.expand_path(File.dirname(__FILE__))

require "#{ep}/ext/em"
require "#{ep}/ext/bytesize"
require "#{ep}/ext/json"

module NATS

  VERSION = "0.4.4".freeze

  DEFAULT_PORT = 4222
  DEFAULT_URI = "nats://localhost:#{DEFAULT_PORT}".freeze

  MAX_RECONNECT_ATTEMPTS = 10
  RECONNECT_TIME_WAIT = 2

  AUTOSTART_PID_FILE = '/tmp/nats-server.pid'
  AUTOSTART_LOG_FILE = '/tmp/nats-server.log'

  # Protocol
  # @private
  MSG      = /\AMSG\s+([^\s]+)\s+([^\s]+)\s+(([^\s]+)[^\S\r\n]+)?(\d+)\r\n/i #:nodoc:
  OK       = /\A\+OK\s*\r\n/i #:nodoc:
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

  # Duplicate autostart protection
  @@tried_autostart = {}

  class Error < StandardError #:nodoc:
  end

  class << self
    attr_reader   :client, :reactor_was_running, :err_cb, :err_cb_overridden #:nodoc:
    attr_accessor :timeout_cb #:nodoc

    alias :reactor_was_running? :reactor_was_running

    # Create and return a connection to the server with the given options.
    # The server will be autostarted if needed if the <b>uri</b> is determined to be local.
    # The optional block will be called when the connection has been completed.
    #
    # @param [Hash] opts
    # @option opts [String] :uri The URI to connect to, example nats://localhost:4222
    # @option opts [Boolean] :autostart Boolean that can be used to suppress autostart functionality.
    # @option opts [Boolean] :reconnect Boolean that can be used to suppress reconnect functionality.
    # @option opts [Boolean] :debug Boolean that can be used to output additional debug information.
    # @option opts [Boolean] :verbose Boolean that is sent to server for setting verbose protocol mode.
    # @option opts [Boolean] :pedantic Boolean that is sent to server for setting pedantic mode.
    # @param [Block] &blk called when the connection is completed. Connection will be passed to the block.
    # @return [NATS] connection to the server.
    def connect(opts={}, &blk)
      # Defaults
      opts[:verbose] = false if opts[:verbose].nil?
      opts[:pedantic] = false if opts[:pedantic].nil?
      opts[:reconnect] = true if opts[:reconnect].nil?

      # Override with ENV
      opts[:uri] ||= ENV['NATS_URI'] || DEFAULT_URI
      opts[:verbose] = ENV['NATS_VERBOSE'] unless ENV['NATS_VERBOSE'].nil?
      opts[:pedantic] = ENV['NATS_PEDANTIC'] unless ENV['NATS_PEDANTIC'].nil?
      opts[:debug] = ENV['NATS_DEBUG'] if !ENV['NATS_DEBUG'].nil?
      opts[:autostart] = (ENV['NATS_NO_AUTOSTART'] ? false : true) if opts[:autostart].nil?
      @uri = opts[:uri] = URI.parse(opts[:uri])
      @err_cb = proc { raise Error, "Could not connect to server on #{@uri}."} unless err_cb
      check_autostart(@uri) if opts[:autostart]
      client = EM.connect(@uri.host, @uri.port, self, opts)
      client.on_connect(&blk) if blk
      return client
    end

    # Create a default client connection to the server.
    # @see NATS::connect
    def start(*args, &blk)
      @reactor_was_running = EM.reactor_running?
      unless (@reactor_was_running || blk)
        raise(Error, "EM needs to be running when NATS.start called without a run block")
      end
      # Setup optimized select versions
      EM.epoll; EM.kqueue
      EM.run { @client = connect(*args, &blk) }
    end

    # Close the default client connection and optionally call the associated block.
    # @param [Block] &blk called when the connection is closed.
    def stop(&blk)
      client.close if (client and client.connected?)
      blk.call if blk
      @@tried_autostart = {}
      @err_cb = nil
    end

    # @return [Boolean] Connected state
    def connected?
      return false unless client
      client.connected?
    end

    # @return [Hash] Options
    def options
      return {} unless client
      client.options
    end

    # Set the default on_error callback.
    # @param [Block] &callback called when an error has been detected.
    def on_error(&callback)
      @err_cb, @err_cb_overridden = callback, true
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
      v = [rand(0x0010000),rand(0x0010000),rand(0x0010000),
           rand(0x0010000),rand(0x0010000),rand(0x1000000)]
      "_INBOX.%04x%04x%04x%04x%04x%06x" % v
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
      system("nats-server #{port_arg} #{user_arg} #{pass_arg} #{log_arg} #{pid_arg} -d 2> /dev/null")
      $? == 0
    end

  end

  attr_reader :connected, :connect_cb, :err_cb, :err_cb_overridden #:nodoc:
  attr_reader :closing, :reconnecting, :options #:nodoc

  alias :connected? :connected
  alias :closing? :closing
  alias :reconnecting? :reconnecting

  def initialize(options)
    @uri = options[:uri]
    @uri.user = options[:user] if options[:user]
    @uri.password = options[:pass] if options[:pass]
    @options = options
    @ssid, @subs = 1, {}
    @err_cb = NATS.err_cb
    @reconnect_timer, @needed = nil, nil
    @connected, @closing, @reconnecting = false, false, false
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

  # Define a callback to be called when a reconnect attempt is being made.
  # @param [Block] &blk called when the connection is closed.
  def on_reconnect(&callback)
    @reconnect_cb = callback
  end

  # Close the connection to the server.
  def close
    @closing = true
    close_connection_after_writing
  end

  def user_err_cb? # :nodoc:
    err_cb_overridden || NATS.err_cb_overridden
  end

  def send_connect_command #:nodoc:
    cs = { :verbose => @options[:verbose], :pedantic => @options[:pedantic] }
    if @uri.user
      cs[:user] = @uri.user
      cs[:pass] = @uri.password
    end
    send_command("CONNECT #{cs.to_json}#{CR_LF}")
  end

  def queue_server_rt(&cb) #:nodoc:
    return unless cb
    (@pongs ||= []) << cb
    send_command(PING_REQUEST)
  end

  def on_msg(subject, sid, reply, msg) #:nodoc:
    return unless sub = @subs[sid]

    # Check for auto_unsubscribe
    sub[:received] += 1
    return unsubscribe(sid) if (sub[:max] && (sub[:received] > sub[:max]))

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
    @pending.each { |p| send_data(p) }
    @pending = nil
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
              when ERR
                @buf = $'
                @err_cb = proc { raise Error, "Error received from server :#{$1}."} unless user_err_cb?
                err_cb.call($1)
              when PING
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
                @err_cb = proc { raise Error, "Error: Ukknown Protocol."} unless user_err_cb?
                err_cb.call($1)
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
  end

  def connection_completed #:nodoc:
    @connected = true
    if reconnecting?
      EM.cancel_timer(@reconnect_timer)
      send_connect_command
      @subs.each_pair { |k, v| send_command("SUB #{v[:subject]} #{k}#{CR_LF}") }
    end
    flush_pending if @pending
    unless user_err_cb? or reconnecting?
      @err_cb = proc { raise Error, "Client disconnected from server on #{@uri}."}
    end
    if (connect_cb and not reconnecting?)
      # We will round trip the server here to make sure all state from any pending commands
      # has been processed before calling the connect callback.
      queue_server_rt { connect_cb.call(self) }
    end
    @reconnecting = false
    @parse_state = AWAITING_CONTROL_LINE
  end

  def schedule_reconnect(wait=RECONNECT_TIME_WAIT) #:nodoc:
    @reconnecting = true
    @reconnect_attempts = 0
    @reconnect_timer = EM.add_periodic_timer(wait) { attempt_reconnect }
  end

  def unbind #:nodoc:
    if connected? and not closing? and not reconnecting? and @options[:reconnect]
      schedule_reconnect
    else
      process_disconnect unless reconnecting?
    end
  end

  def process_disconnect #:nodoc:
    if not closing? and @err_cb
      err_string = @connected ? "Client disconnected from server on #{@uri}." : "Could not connect to server on #{@uri}"
      err_cb.call(err_string)
    end
  ensure
    EM.cancel_timer(@reconnect_timer) if @reconnect_timer
    if (NATS.client == self and connected? and closing? and not NATS.reactor_was_running?)
      EM.stop
    end
    @connected = @reconnecting = false
    true # Chaining
  end

  def attempt_reconnect #:nodoc:
    process_disconnect and return if (@reconnect_attempts += 1) > MAX_RECONNECT_ATTEMPTS
    EM.reconnect(@uri.host, @uri.port, self)
  end

  def send_command(command) #:nodoc:
    queue_command(command) and return unless connected?
    send_data(command)
  end

  def queue_command(command) #:nodoc:
    (@pending ||= []) << command
    true
  end

  def inspect #:nodoc:
    "<nats client v#{NATS::VERSION}>"
  end

end

