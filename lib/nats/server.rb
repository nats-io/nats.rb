
require File.dirname(__FILE__) + '/ext/em'
require File.dirname(__FILE__) + '/ext/bytesize'
require File.dirname(__FILE__) + '/ext/json'
require File.dirname(__FILE__) + '/server/sublist'
require File.dirname(__FILE__) + '/server/options'
require File.dirname(__FILE__) + '/server/const'

require 'socket'
require 'fileutils'
require 'pp'

module NATS

  # Subscriber
  Subscriber = Struct.new(:conn, :subject, :sid)

  class Server
    
    class << self
      attr_reader :id, :info, :log_time, :auth_required, :debug_flag, :trace_flag
      alias auth_required? :auth_required
      alias debug_flag? :debug_flag
      alias trace_flag? :trace_flag
 
      def version; "nats server version #{NATS::VERSION}" end

      def host; @options[:addr]  end
      def port; @options[:port]  end
      def pid_file; @options[:pid_file] end

      def setup(argv)
        @options = {}

        parser.parse!(argv)
        read_config_file
        finalize_options

        @id, @cid = fast_uuid, 1
        @sublist = Sublist.new
        @info = {
          :nats_server_id => Server.id,
          :version => VERSION,
          :auth_required => auth_required?,
          :max_payload => MAX_PAYLOAD_SIZE
        }

        # Write pid file if need be.
        File.open(@options[:pid_file], 'w') { |f| f.puts "#{Process.pid}" } if @options[:pid_file]

        # Check for daemon flag
        if @options[:daemonize]
          require 'rubygems'
          require 'daemons'
          # These log messages visible to controlling TTY
          log "Starting #{NATS::APP_NAME} version #{NATS::VERSION} on port #{NATS::Server.port}"
          log "Switching to daemon mode"
          Daemons.daemonize(:app_name => APP_NAME, :mode => :exec)
        end

        setup_logs

        # Setup optimized select versions 
        EM.epoll unless @options[:noepoll]
        EM.kqueue unless @options[:nokqueue]

      end
      
      def subscribe(subscriber)
        @sublist.insert(subscriber.subject, subscriber)
      end

      def unsubscribe(subscriber)
        @sublist.remove(subscriber.subject, subscriber)
      end
      
      def route_to_subscribers(subject, reply, msg)
        @sublist.match(subject).each do |subscriber|
          # Skip anyone in the closing state
          next if subscriber.conn.closing
          
          trace "Matched subscriber", subscriber[:subject], subscriber[:sid], subscriber.conn.client_info
          subscriber.conn.send_data("MSG #{subject} #{subscriber.sid} #{reply} #{msg.bytesize}#{CR_LF}") 
          subscriber.conn.send_data(msg)
          subscriber.conn.send_data(CR_LF)

          # Check the outbound queue here and react if need be..
          if subscriber.conn.get_outbound_data_size > MAX_OUTBOUND_SIZE
            subscriber.conn.error_close SLOW_CONSUMER
            log "Slow consumer dropped", subscriber.conn.client_info
          end
        end
      end

      def auth_ok?(user, pass)
        user == @options[:user] && pass == @options[:pass]
      end

      def cid
        @cid+=1
      end

      def info_string
        @info.to_json
      end

    end    
  end
  
  module Connection

    attr_reader :cid, :closing
    
    def client_info
      @client_info ||= Socket.unpack_sockaddr_in(get_peername)
    end

    def post_init
      @cid = Server.cid
      @subscriptions = {}
      @verbose = @pedantic = true # suppressed by most clients, but allows friendly telnet
      @receive_data_calls = 0
      send_info
      @auth_pending = EM.add_timer(AUTH_TIMEOUT) { connect_auth_timeout } if Server.auth_required?
      debug "Client connection created", client_info, cid
    end

    def connect_auth_timeout
      error_close AUTH_REQUIRED
      debug "Connection timeout due to lack of auth credentials", cid
    end

    def receive_data(data)
      @receive_data_calls += 1
      (@buf ||= '') << data
      close_connection and return if @buf =~ /(\006|\004)/ # ctrl+c or ctrl+d for telnet friendly
      while (@buf && !@buf.empty? && !@closing)
        if (@msg_size && @buf.bytesize >= (@msg_size + CR_LF_SIZE))
          msg = @buf.slice(0, @msg_size)
          process_msg(msg)
          @buf = @buf.slice((msg.bytesize + CR_LF_SIZE), @buf.bytesize)
        elsif @buf =~ /^(.*)\r\n/
          @buf = $'          
          process_op($1)
        else # Waiting for additional data
          return
        end
      end
      # Nothing should be here.
    end
      
    def process_op(op)
      case op
        when PUB_OP
          ctrace 'PUB OP', op
          return if @auth_pending
          @pub_sub, @reply, @msg_size, = $1, $3, $4.to_i
          send_data PAYLOAD_TOO_BIG and return if (@msg_size > MAX_PAYLOAD_SIZE)
          send_data INVALID_SUBJECT and return if @pedantic && !(@pub_sub =~ SUB_NO_WC)
        when SUB_OP
          ctrace 'SUB OP', op
          return if @auth_pending
          sub, sid = $1, $2          
          send_data INVALID_SUBJECT and return if !($1 =~ SUB)          
          send_data INVALID_SID_TAKEN and return if @subscriptions[sid]
          subscriber = Subscriber.new(self, sub, sid)
          @subscriptions[sid] = subscriber
          Server.subscribe(subscriber)
          send_data OK if @verbose
        when UNSUB_OP
          ctrace 'UNSUB OP', op
          return if @xsauth_pending
          sid, subscriber = $1, @subscriptions[$1]
          send_data INVALID_SID_NOEXIST and return unless subscriber
          Server.unsubscribe(subscriber)
          @subscriptions.delete(sid)
          send_data OK if @verbose
        when PING
          ctrace 'PING OP', op
          send_data PONG_RESPONSE
        when CONNECT
          ctrace 'CONNECT OP', op
          begin
            config = JSON.parse($1, :symbolize_keys => true)
            process_connect_config(config)
          rescue => e
            send_data INVALID_CONFIG
            log_error
          end
        when INFO
          ctrace 'INFO OP', op
          send_info
        else
          ctrace 'Unknown Op', op
          send_data UNKNOWN_OP
      end
    end

    def send_info
      send_data "INFO #{Server.info_string}#{CR_LF}"          
    end

    def process_msg(body)
      ctrace 'Processing msg', @pub_sub, @reply, body
      send_data OK if @verbose
      Server.route_to_subscribers(@pub_sub, @reply, body)
      @pub_sub = @msg_size = @reply = nil
      true
    end
    
    def process_connect_config(config)
      @verbose  = config[:verbose] if config[:verbose] != nil
      @pedantic = config[:pedantic] if config[:pedantic] != nil

      send_data OK and return unless Server.auth_required?

      EM.cancel_timer(@auth_pending)
      if Server.auth_ok?(config[:user], config[:pass])
        send_data OK
        @auth_pending = nil
      else
        error_close AUTH_FAILED
        debug "Authorization failed for connection", cid
      end
    end
    
    def error_close(msg)
      send_data msg
      close_connection_after_writing
      @closing = true
    end
    
    def unbind
      debug "Client connection closed", client_info, cid
      ctrace "Receive_Data called #{@receive_data_calls} times." if @receive_data_calls > 0
      @subscriptions.each_value { |subscriber| Server.unsubscribe(subscriber) }
      EM.cancel_timer(@auth_pending) if @auth_pending
      @auth_pending = nil
    end

    def ctrace(*args)
      trace(args, "c: #{cid}")
    end
  end

end

def fast_uuid
  v = [rand(0x0010000),rand(0x0010000),rand(0x0010000),
       rand(0x0010000),rand(0x0010000),rand(0x1000000)]
  "%04x%04x%04x%04x%04x%06x" % v
end

def log(*args)
  args.unshift(Time.now) if NATS::Server.log_time
  pp args.compact
end

def debug(*args)
  log *args if NATS::Server.debug_flag?
end

def trace(*args)
  log *args if NATS::Server.trace_flag?
end

def log_error(e=$!)
  debug e, e.backtrace
end

def shutdown
  puts
  log 'Server exiting..'
  EM.stop
  FileUtils.rm(NATS::Server.pid_file) if NATS::Server.pid_file
  exit
end

['TERM','INT'].each { |s| trap(s) { shutdown } }

# Do setup
NATS::Server.setup(ARGV.dup)

# Event Loop

EM.run {

  log "Starting #{NATS::APP_NAME} version #{NATS::VERSION} on port #{NATS::Server.port}"

  begin
    EM.set_descriptor_table_size(32768) # Requires Root privileges    
    EventMachine::start_server(NATS::Server.host, NATS::Server.port, NATS::Connection)
  rescue => e
    log "Could not start server on port #{NATS::Server.port}"
    log_error
    exit
  end

}
