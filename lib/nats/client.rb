
require 'uri'
require File.dirname(__FILE__) + '/ext/em'
require File.dirname(__FILE__) + '/ext/bytesize'
require File.dirname(__FILE__) + '/ext/json'

class NATS < EM::Connection
  
  VERSION = "0.1".freeze

  DEFAULT_URI = 'nats://localhost:8222'

  CR_LF = "\r\n".freeze
  CR_LF_SIZE = CR_LF.bytesize

  PONG_RESPONSE = "PONG#{CR_LF}".freeze

  MAX_RECONNECT_ATTEMPTS = 15
  RECONNECT_TIME_WAIT = 2 # in secs

  # Pedantic Mode
  SUB = /^([^\.\*>\s]+|>$|\*)(\.([^\.\*>\s]+|>$|\*))*$/
  SUB_NO_WC = /^([^\.\*>\s]+)(\.([^\.\*>\s]+))*$/
    
  class << self
    attr_reader :client, :reactor_was_running, :err_cb, :err_cb_overridden
    alias :reactor_was_running? :reactor_was_running

    def connect(options = {})
      server = (options[:uri] || ENV['NATS_URI'] || DEFAULT_URI)
      debug  = (options[:debug] || ENV['NATS_DEBUG'])
      uri = URI.parse(server)
      @err_cb = proc { raise "Could not connect to server on #{uri}."} unless @err_cb
      EM.connect(uri.host, uri.port, self, :uri => uri, :debug => debug)
    end
    
    def start(*args, &blk)
      @reactor_was_running = EM.reactor_running?
      EM.run {
        @client = connect(*args)
        @client.on_connect(&blk) if blk
      }
    end
    
    def stop
      client.close if client
    end

    def on_error(&callback)
      @err_cb, @err_cb_overridden = callback, true
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
    send_command("CONNECT #{connect_string}#{CR_LF}")
  end
  
  def publish(subject, data)
    data = data.to_s
    send_command("PUB #{subject} #{data.bytesize}#{CR_LF}#{data}#{CR_LF}")
  end
    
  def subscribe(subject, &callback)
    @sid += 1    
    @subs[@sid] = {:subject => subject, :callback => callback}
    send_command("SUB #{subject} #{@sid}#{CR_LF}")
    @sid
  end

  def unsubscribe(sid)
    @subs.delete(sid)
    send_command("UNSUB #{sid}#{CR_LF}")
  end
    
  def connect_string
    cs = { :noreply => true }
    if @uri.user
      cs[:user] = @uri.user
      cs[:pass] = @uri.password
    end
    cs.to_json
  end

  def on_connect(&callback)
    @connect_cb = callback
  end

  def on_error(&callback)
    @err_cb, @err_cb_overridden = callback, true
  end

  def user_err_cb?
    err_cb_overridden || NATS.err_cb_overridden
  end

  def on_reconnect(&callback)
    @reconnect_cb = callback
  end
  
  def close
    @closing = true
    close_connection_after_writing
  end
  
  def on_msg(subject, sid, msg)
    return unless subscriber = @subs[sid]
    subscriber[:callback].call(subject, msg)
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
        on_msg(@sub, @sid, payload)    
        @buf = @buf.slice((@needed + CR_LF_SIZE), @buf.bytesize)          
        @sub = @sid = @needed = nil
      elsif @buf =~ /^(.*)\r\n/ # Process a control line
        @buf = $'
        op = $1
        case op
          when /^MSG\s+(\S+)\s+(\S+)\s+(\d+)$/i
            @sub, @sid, @needed = $1, $2.to_i, $3.to_i
          when /^OK/i
          when /^ERR\s+('.+')/i
            @err_cb = proc { raise "Error received from server :#{$1}."} unless user_err_cb?
            err_cb.call($1)
          when /^PING/i
            send_command(PONG_RESPONSE)
          when /^INFO\s+(.+)/i
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
       send_command("CONNECT #{connect_string}#{CR_LF}")
       @subs.each_pair { |k, v| send_command("SUB #{v[:subject]} #{k}#{CR_LF}") }
     end
     flush_pending if @pending
     connect_cb.call(self) if @connect_cb and not reconnecting?
     @err_cb = proc { raise "Client disconnected from server on #{@uri}."} unless user_err_cb? or reconnecting?
     @reconnecting = false
   end
  
  def unbind
    if connected? and not closing? and not reconnecting?
      @reconnecting = true
      @reconnect_attempts = 0
      @reconnect_timer = EM.add_periodic_timer(RECONNECT_TIME_WAIT) { attempt_reconnect }
    else
      process_disconnect unless reconnecting?
    end
  end
    
  def process_disconnect
    if not closing? and @err_cb
      err_string = @connected ? "Client disconnected from server on #{@uri}." : "Could not connect to server on #{@uri}"
      err_cb.call(err_string) if @err_cb and not closing?
    end
    ensure
    stop_em = (NATS.client == self and connected? and closing? and not NATS.reactor_was_running?)
    EM.stop if stop_em
    @connected = @reconnecting = false
    EM.cancel_timer(@reconnect_timer) if @reconnect_timer
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
    "#<nats client v#{NATS::VERSION}>"
  end
  
end

