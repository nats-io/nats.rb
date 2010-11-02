
require 'uri'

require File.dirname(__FILE__) + '/ext/em'
require File.dirname(__FILE__) + '/ext/bytesize'
require File.dirname(__FILE__) + '/ext/json'

module NATS

  VERSION = "0.3.4".freeze

  DEFAULT_PORT = 4222
  DEFAULT_URI = "nats://localhost:#{DEFAULT_PORT}".freeze

  CR_LF = "\r\n".freeze
  CR_LF_SIZE = CR_LF.bytesize

  PING_REQUEST  = "PING#{CR_LF}".freeze
  PONG_RESPONSE = "PONG#{CR_LF}".freeze

  MAX_RECONNECT_ATTEMPTS = 10
  RECONNECT_TIME_WAIT = 2 # in secs

  # Protocol
  MSG  = /^MSG\s+(\S+)\s+(\S+)\s+((\S+)\s+)?(\d+)$/i
  OK   = /^\+OK/i
  ERR  = /^-ERR\s+('.+')?/i
  PING = /^PING/i
  PONG = /^PONG/i  
  INFO = /^INFO\s+(.+)/i

  # Pedantic Mode
  SUB = /^([^\.\*>\s]+|>$|\*)(\.([^\.\*>\s]+|>$|\*))*$/
  SUB_NO_WC = /^([^\.\*>\s]+)(\.([^\.\*>\s]+))*$/

  # Duplicate autostart protection
  @@tried_autostart = {}

  class Error < StandardError; end

  class << self
    attr_reader :client, :reactor_was_running, :err_cb, :err_cb_overridden
    alias :reactor_was_running? :reactor_was_running

    def connect(options = {}, &blk)
      options[:uri] ||= ENV['NATS_URI'] || DEFAULT_URI
      options[:debug] ||= ENV['NATS_DEBUG']
      options[:autostart] ||= ENV['NATS_AUTO'] || true
      uri = options[:uri] = URI.parse(options[:uri])
      @err_cb = proc { raise Error, "Could not connect to server on #{uri}."} unless @err_cb
      check_autostart(uri) if options[:autostart]
      client = EM.connect(uri.host, uri.port, self, options)
      client.on_connect(&blk) if blk
      return client
    end
    
    def start(*args, &blk)
      @reactor_was_running = EM.reactor_running?
      unless (@reactor_was_running || blk)
        err = "EM needs to be running when NATS.start called without a run block"
        @err_cb ? @err_cb.call(err) : raise(Error, err)
      end
      EM.run { @client = connect(*args, &blk) }
    end
    
    def stop(&blk)
      client.close if (client and client.connected?)
      blk.call if blk
    end

    def on_error(&callback)
      @err_cb, @err_cb_overridden = callback, true
    end

    # Mirror instance methods for our client
    def publish(*args, &blk)
      (@client ||= connect).publish(*args, &blk)
    end

    def subscribe(*args, &blk)
      (@client ||= connect).subscribe(*args, &blk)
    end

    def unsubscribe(*args)
      (@client ||= connect).unsubscribe(*args)
    end

    def request(*args, &blk)
      (@client ||= connect).request(*args, &blk)
    end

    # utils
    def create_inbox
      v = [rand(0x0010000),rand(0x0010000),rand(0x0010000),
           rand(0x0010000),rand(0x0010000),rand(0x1000000)]
      "_INBOX.%04x%04x%04x%04x%04x%06x" % v
    end

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
      log_arg  = '-l /tmp/nats-server.log'
      pid_arg  = '-P /tmp/nats-server.pid'
      # daemon mode to release client
      system("nats-server #{port_arg} #{user_arg} #{pass_arg} #{log_arg} #{pid_arg} -d 2> /dev/null")
      $? == 0
    end
    
    def wait_for_server(uri)
      start = Time.now
      while (Time.now - start < 5) # Wait 5 seconds max
        break if server_running?(uri)
        sleep(0.1)
      end
    end

    def server_running?(uri)
      require 'socket'
      s = TCPSocket.new(uri.host, uri.port)
      s.close
      return true
    rescue
      return false
    end

  end
  
  attr_reader :connect_cb, :err_cb, :err_cb_overridden, :connected, :closing, :reconnecting
  
  alias :connected? :connected
  alias :closing? :closing
  alias :reconnecting? :reconnecting  
  
  def initialize(options)
    @uri = options[:uri]
    @debug = options[:debug]
    @sid, @subs = 1, {}
    @err_cb = NATS.err_cb
    send_connect_command
  end
  
  def publish(subject, data='', opt_reply=nil, &blk)
    data = data.to_s
    send_command("PUB #{subject} #{opt_reply} #{data.bytesize}#{CR_LF}#{data}#{CR_LF}")
    queue_server_rt(&blk) if blk
  end
    
  def subscribe(subject, &callback)
    @sid += 1    
    @subs[@sid] = { :subject => subject, :callback => callback }
    send_command("SUB #{subject} #{@sid}#{CR_LF}")
    @sid
  end

  def unsubscribe(sid)
    @subs.delete(sid)
    send_command("UNSUB #{sid}#{CR_LF}")
  end
    
  def request(subject, data=nil, opts={}, &callback)
    inbox = NATS.create_inbox
    s = subscribe(inbox) { |msg| callback.call(msg) }
    publish(subject, data, inbox)
    return s
  end

  def send_connect_command
    cs = { :verbose => false, :pedantic => false }
    if @uri.user
      cs[:user] = @uri.user 
      cs[:pass] = @uri.password
    end
    send_command("CONNECT #{cs.to_json}#{CR_LF}")
  end

  def queue_server_rt(&cb)
    return unless cb
    (@pongs ||= []) << cb 
    send_command(PING_REQUEST)
  end

  def on_connect(&callback)
    @connect_cb = callback
  end

  def on_error(&callback)
    @err_cb, @err_cb_overridden = callback, true
  end

  def on_reconnect(&callback)
    @reconnect_cb = callback
  end

  def user_err_cb?
    err_cb_overridden || NATS.err_cb_overridden
  end
  
  def close
    @closing = true
    close_connection_after_writing
  end
  
  def on_msg(subject, sid, reply, msg)
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

  def flush_pending
    return unless @pending      
    @pending.each { |p| send_data(p) }
    @pending = nil
  end
  
  def receive_data(data)
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

  def process_info(info)
    @server_info = JSON.parse(info, :symbolize_keys => true)
  end

  def connection_completed
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

  def schedule_reconnect(wait=RECONNECT_TIME_WAIT)
    @reconnecting = true
    @reconnect_attempts = 0
    @reconnect_timer = EM.add_periodic_timer(wait) { attempt_reconnect }
  end

  def unbind
    if connected? and not closing? and not reconnecting?
      schedule_reconnect
    else
      process_disconnect unless reconnecting?
    end
  end
    
  def process_disconnect
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
  
  def attempt_reconnect
    process_disconnect and return if (@reconnect_attempts += 1) > MAX_RECONNECT_ATTEMPTS
    EM.reconnect(@uri.host, @uri.port, self)
  end
  
  def send_command(command)
    queue_command(command) and return unless connected?
    send_data(command)
  end
    
  def queue_command(command)
    (@pending ||= []) << command
    true
  end

  def inspect
    "<nats client v#{NATS::VERSION}>"
  end
  
end

