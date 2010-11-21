
require 'uri'

require File.dirname(__FILE__) + '/ext/em'
require File.dirname(__FILE__) + '/ext/bytesize'
require File.dirname(__FILE__) + '/ext/json'

# NATS is a simple publish-subscribe messaging system.
#
# == Usage
# <tt>
#    require "nats/client"
#
#    NATS.start do
#
#      # Simple Subscriber
#      NATS.subscribe('foo') { |msg| puts "Msg received : '#{msg}'" }
#
#      # Simple Publisher
#      NATS.publish('foo.bar.baz', 'Hello World!')
#
#      # Publish with closure, callback fires when server has processed the message
#      NATS.publish('foo', 'You done?') { puts 'msg processed!' }
#
#      # Unsubscribing
#      s = NATS.subscribe('bar') { |msg| puts "Msg received : '#{msg}'" }
#      NATS.unsubscribe(s)
#
#      # Request/Response
#
#      # The helper
#      NATS.subscribe('help') do |msg, reply|
#        NATS.publish(reply, "I'll help!")
#      end
#
#      # Help request
#      NATS.request('help') { |response|
#        puts "Got a response: '#{response}'"
#      }
#
#      # Wildcard Subscriptions
#
#      # '*" matches any token
#      NATS.subscribe('foo.*.baz') { |msg, _, sub| puts "Msg received on [#{sub}] : '#{msg}'" }
#
#      # '>" can only be last token, and matches to any depth
#      NATS.subscribe('foo.>') { |msg, _, sub| puts "Msg received on [#{sub}] : '#{msg}'" }
#
#
#      # Stop using NATS.stop, exits EM loop if NATS.start started it
#      NATS.stop
#
#    end
#
# </tt>


module NATS

  # Version <b>0.3.12</b>
  VERSION = "0.3.12".freeze

  # Default port: <b>4222</b>
  DEFAULT_PORT = 4222

  # Default URI to connect to the server, <b>nats://localhost:4222</b>
  DEFAULT_URI = "nats://localhost:#{DEFAULT_PORT}".freeze

  # Max attempts at a reconnect: <b>10</b>
  MAX_RECONNECT_ATTEMPTS = 10

  # Maximum time to wait for a reconnect: <b>2 seconds</b>
  RECONNECT_TIME_WAIT = 2

  # Protocol
  MSG  = /^MSG\s+(\S+)\s+(\S+)\s+((\S+)\s+)?(\d+)$/i #:nodoc:
  OK   = /^\+OK/i #:nodoc:
  ERR  = /^-ERR\s+('.+')?/i #:nodoc:
  PING = /^PING/i #:nodoc:
  PONG = /^PONG/i #:nodoc:
  INFO = /^INFO\s+(.+)/i #:nodoc:

  # Responses
  CR_LF = ("\r\n".freeze) #:nodoc:
  CR_LF_SIZE = (CR_LF.bytesize) #:nodoc:

  PING_REQUEST  = ("PING#{CR_LF}".freeze) #:nodoc:
  PONG_RESPONSE = ("PONG#{CR_LF}".freeze) #:nodoc:

  EMPTY_MSG = (''.freeze) #:nodoc:

  # Used for future pedantic Mode
  SUB = /^([^\.\*>\s]+|>$|\*)(\.([^\.\*>\s]+|>$|\*))*$/ #:nodoc:
  SUB_NO_WC = /^([^\.\*>\s]+)(\.([^\.\*>\s]+))*$/ #:nodoc:

  # Duplicate autostart protection
  @@tried_autostart = {}

  class Error < StandardError #:nodoc:
  end

  class << self
    attr_reader :client, :reactor_was_running, :err_cb, :err_cb_overridden #:nodoc:
    alias :reactor_was_running? :reactor_was_running

    # Create and return a connection to the server with the given options. The server will be autostarted if needed if
    # the <b>uri</b> is determined to be local. The optional block will be called when the connection has been completed.
    #
    def connect(options = {}, &blk)
      options[:uri] ||= ENV['NATS_URI'] || DEFAULT_URI
      options[:debug] ||= ENV['NATS_DEBUG']
      options[:autostart] = (ENV['NATS_AUTO'] || true) unless options[:autostart] != nil
      uri = options[:uri] = URI.parse(options[:uri])
      @err_cb = proc { raise Error, "Could not connect to server on #{uri}."} unless err_cb
      check_autostart(uri) if options[:autostart]
      client = EM.connect(uri.host, uri.port, self, options)
      client.on_connect(&blk) if blk
      return client
    end

    # Create a default client connection to the server. See connect for more information.
    def start(*args, &blk)
      @reactor_was_running = EM.reactor_running?
      unless (@reactor_was_running || blk)
        raise(Error, "EM needs to be running when NATS.start called without a run block")
      end
      EM.run { @client = connect(*args, &blk) }
    end

    # Close the default client connection and optionally call the associated block.
    def stop(&blk)
      client.close if (client and client.connected?)
      blk.call if blk
    end

    # Set the default on_error callback.
    def on_error(&callback)
      @err_cb, @err_cb_overridden = callback, true
    end

    # Publish a message using the default client connection. See NATS#publish for more information.
    def publish(*args, &blk)
      (@client ||= connect).publish(*args, &blk)
    end

    # Subscribe using the default client connection. See NATS#subscribe for more information.
    def subscribe(*args, &blk)
      (@client ||= connect).subscribe(*args, &blk)
    end

    # Cancel a subscription on the default client connection.
    def unsubscribe(*args)
      (@client ||= connect).unsubscribe(*args)
    end

    # Publish a message and wait for a response on the default client connection. See NATS#request for more information.
    def request(*args, &blk)
      (@client ||= connect).request(*args, &blk)
    end

    # Returns a subject that can be used for "directed" communications, utilized in #request.
    def create_inbox
      v = [rand(0x0010000),rand(0x0010000),rand(0x0010000),
           rand(0x0010000),rand(0x0010000),rand(0x1000000)]
      "_INBOX.%04x%04x%04x%04x%04x%06x" % v
    end

    def check_autostart(uri) #:nodoc:
      return if uri_is_remote?(uri) || @@tried_autostart[uri]
      @@tried_autostart[uri] = true
      return if server_running?(uri)
      return unless try_autostart_succeeded?(uri)
      wait_for_server(uri)
    end

    def uri_is_remote?(uri) #:nodoc:
      uri.host != 'localhost' && uri.host != '127.0.0.1'
    end

    def try_autostart_succeeded?(uri) #:nodoc:
      port_arg = "-p #{uri.port}"
      user_arg = "--user #{uri.user}" if uri.user
      pass_arg = "--pass #{uri.password}" if uri.password
      log_arg  = '-l /tmp/nats-server.log'
      pid_arg  = '-P /tmp/nats-server.pid'
      # daemon mode to release client
      system("nats-server #{port_arg} #{user_arg} #{pass_arg} #{log_arg} #{pid_arg} -d 2> /dev/null")
      $? == 0
    end

    def wait_for_server(uri) #:nodoc:
      start = Time.now
      while (Time.now - start < 5) # Wait 5 seconds max
        break if server_running?(uri)
        sleep(0.1)
      end
    end

    def server_running?(uri) #:nodoc:
      require 'socket'
      s = TCPSocket.new(uri.host, uri.port)
      s.close
      return true
    rescue
      return false
    end

  end

  attr_reader :connect_cb, :err_cb, :err_cb_overridden, :connected, :closing, :reconnecting #:nodoc:

  alias :connected? :connected
  alias :closing? :closing
  alias :reconnecting? :reconnecting

  def initialize(options)
    @uri = options[:uri]
    @debug = options[:debug]
    @ssid, @subs = 1, {}
    @err_cb = NATS.err_cb
    @reconnect_timer, @needed = nil, nil
    @connected, @closing, @reconnecting = false, false, false
    send_connect_command
  end

  # Publish a message to a given subject, with optional reply subject and completion block
  def publish(subject, data=EMPTY_MSG, opt_reply=nil, &blk)
    return unless subject
    data = data.to_s
    send_command("PUB #{subject} #{opt_reply} #{data.bytesize}#{CR_LF}#{data}#{CR_LF}")
    queue_server_rt(&blk) if blk
  end

  # Subscribe to a subject with optional wildcards. Messages will be delivered to the supplied callback.
  # Callback can take any number of the supplied arguments as defined by the list: msg, reply, sub.
  # Returns subscription id which can be passed to NATS#unsubscribe.
  def subscribe(subject, &callback)
    return unless subject
    @ssid += 1
    @subs[@ssid] = { :subject => subject, :callback => callback }
    send_command("SUB #{subject} #{@ssid}#{CR_LF}")
    @ssid
  end

  # Cancel a subscription.
  def unsubscribe(sid)
    @subs.delete(sid)
    send_command("UNSUB #{sid}#{CR_LF}")
  end

  # Send a request and have the response delivered to the supplied callback.
  # Returns subscription id which can be passed to NATS#unsubscribe.
  def request(subject, data=nil, &cb)
    return unless subject
    inbox = NATS.create_inbox
    s = subscribe(inbox) { |msg, reply|
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
  def on_connect(&callback)
    @connect_cb = callback
  end

  # Define a callback to be called when errors occur on the client connection.
  def on_error(&callback)
    @err_cb, @err_cb_overridden = callback, true
  end

  # Define a callback to be called when a reconnect attempt is being made.
  def on_reconnect(&callback)
    @reconnect_cb = callback
  end

  # Close the connection to the server.
  def close
    @closing = true
    close_connection_after_writing
  end

  def user_err_cb? #:nodoc:
    err_cb_overridden || NATS.err_cb_overridden
  end

  def send_connect_command #:nodoc:
    cs = { :verbose => false, :pedantic => false }
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
    return unless subscriber = @subs[sid]
    if cb = subscriber[:callback]
      case cb.arity
        when 0 then cb.call
        when 1 then cb.call(msg)
        when 2 then cb.call(msg, reply)
        else cb.call(msg, reply, subject)
      end
    end
  end

  def flush_pending #:nodoc:
    return unless @pending
    @pending.each { |p| send_data(p) }
    @pending = nil
  end

  def receive_data(data) #:nodoc:
    (@buf ||= '') << data
    while (@buf && !@buf.empty?)
      if (@needed && @buf.bytesize >= @needed + CR_LF_SIZE)
        payload = @buf.slice(0, @needed)
        on_msg(@sub, @sid, @reply, payload)
        @buf = @buf.slice((@needed + CR_LF_SIZE), @buf.bytesize)
        @sub = @sid = @reply = @needed = nil
      elsif @buf =~ /^(.*)\r\n/ # Process a control line
        @buf = $'
        op = $1
        case op
          when MSG
            @sub, @sid, @reply, @needed = $1, $2.to_i, $4, $5.to_i
          when OK # No-op right now
          when ERR
            @err_cb = proc { raise Error, "Error received from server :#{$1}."} unless user_err_cb?
            err_cb.call($1)
          when PING
            send_command(PONG_RESPONSE)
          when PONG
            cb = @pongs.shift
            cb.call if cb
          when INFO
            process_info($1)
        end
      else # Waiting for additional data
        return
      end
    end
  end

  def process_info(info) #:nodoc:
    @server_info = JSON.parse(info, :symbolize_keys => true)
  end

  def connection_completed #:nodoc:
    @connected = true
    if reconnecting?
      EM.cancel_timer(@reconnect_timer)
      send_connect_command
      @subs.each_pair { |k, v| send_command("SUB #{v[:subject]} #{k}#{CR_LF}") }
    end
    flush_pending if @pending
    @err_cb = proc { raise Error, "Client disconnected from server on #{@uri}."} unless user_err_cb? or reconnecting?
    if (connect_cb and not reconnecting?)
      # We will round trip the server here to make sure all state from any pending commands
      # has been processed before calling the connect callback.
      queue_server_rt { connect_cb.call(self) }
    end
    @reconnecting = false
  end

  def schedule_reconnect(wait=RECONNECT_TIME_WAIT) #:nodoc:
    @reconnecting = true
    @reconnect_attempts = 0
    @reconnect_timer = EM.add_periodic_timer(wait) { attempt_reconnect }
  end

  def unbind #:nodoc:
    if connected? and not closing? and not reconnecting?
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
    EM.stop if (NATS.client == self and connected? and closing? and not NATS.reactor_was_running?)
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

