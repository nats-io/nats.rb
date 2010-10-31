
require File.dirname(__FILE__) + '/ext/em'
require File.dirname(__FILE__) + '/ext/bytesize'
require File.dirname(__FILE__) + '/ext/json'
require File.dirname(__FILE__) + '/server/sublist'
require File.dirname(__FILE__) + '/server/parser'

require 'socket'
require 'pp'

module NATS
  
  VERSION = "0.2.0".freeze
    
  # Ops
  INFO = /^INFO$/i
  PUB_OP = /^PUB\s+(\S+)\s+(\d+)(\s+REPLY)?$/i
  SUB_OP = /^SUB\s+(\S+)\s+(\S+)$/i
  UNSUB_OP = /^UNSUB\s+(\S+)$/i  
  PING = /^PING$/i
  CONNECT = /^CONNECT\s+(.+)$/i
  
  # Should be using something different if > 1MB payload
  MAX_PAYLOAD_SIZE = 1024 * 1024

  # RESPONSES
  CR_LF = "\r\n".freeze
  CR_LF_SIZE = CR_LF.bytesize
  OK = "+OK #{CR_LF}".freeze  
  PONG_RESPONSE = "PONG#{CR_LF}".freeze

  INFO_RESPONSE = "#{CR_LF}".freeze

  # ERR responses
  PAYLOAD_TOO_BIG = "-ERR 'Payload size exceeded, max is #{MAX_PAYLOAD_SIZE} bytes'#{CR_LF}".freeze
  INVALID_SUBJECT = "-ERR 'Invalid Subject'#{CR_LF}".freeze
  INVALID_SID_TAKEN = "-ERR 'Invalid Subject Identifier (sid), already taken'#{CR_LF}".freeze
  INVALID_SID_NOEXIST = "-ERR 'Invalid Subject-Identifier (sid), no subscriber registered'#{CR_LF}".freeze
  INVALID_CONFIG = "-ERR 'Invalid config, valid JSON required for connection configuration'#{CR_LF}".freeze
  AUTH_REQUIRED = "-ERR 'Authorization is required'#{CR_LF}".freeze
  AUTH_FAILED = "-ERR 'Authorization failed'#{CR_LF}".freeze

  # Pedantic Mode
  SUB = /^([^\.\*>\s]+|>$|\*)(\.([^\.\*>\s]+|>$|\*))*$/
  SUB_NO_WC = /^([^\.\*>\s]+)(\.([^\.\*>\s]+))*$/

  # Subscriber
  Subscriber = Struct.new(:conn, :subject, :sid)

  # Autorization wait time
  AUTH_TIMEOUT = 5

  class Server

    DEFAULT_PORT = 8222

    class << self
      attr_reader :id, :info, :log_time, :auth_required, :debug_flag, :trace_flag
      alias auth_required? :auth_required
      alias debug_flag? :debug_flag
      alias trace_flag? :trace_flag
 
      def version; "nats server version #{NATS::VERSION}" end
      def host; @options[:addr]  end
      def port; @options[:port]  end

      def setup(argv)
        @options = {}
        parser.parse!(argv)
        read_config_file
        @options[:port] ||= DEFAULT_PORT
        @options[:addr] ||= '0.0.0.0'
        @debug_flag = @options[:debug]
        @trace_flag = @options[:trace]
        @auth_required = (@options[:user] != nil)
        debug @options # Block pass?
        debug "DEBUG is on"
        trace "TRACE is on"
        @log_time = @options[:log_time]
        $stdout = File.new(@options[:log_file], 'w') if @options[:log_file]

        @id = fast_uuid
        @info = {
          :nats_server_id => Server.id,
          :version => VERSION,
          :auth_required => auth_required?,
          :max_payload => MAX_PAYLOAD_SIZE
        }
        @sublist = Sublist.new
      end
      
      def subscribe(subscriber)
        @sublist.insert(subscriber.subject, subscriber)
      end

      def unsubscribe(subscriber)
        @sublist.remove(subscriber.subject, subscriber)
      end
      
      def route_to_subscribers(subject, msg)
        @sublist.match(subject).each do |subscriber|
          trace "Matched subscriber", subscriber[:subject], subscriber[:sid], subscriber.conn.client_info
          subscriber.conn.send_data("MSG #{subject} #{subscriber.sid} #{msg.bytesize}#{CR_LF}#{msg}#{CR_LF}") 
        end
      end

      def auth_ok?(user, pass)
pp user
pp pass
pp @options
        user == @options[:user] && pass == @options[:pass]
      end

      def info_string
        @info.to_json
      end

    end    
  end
  
  module Connection

    def client_info
      @client_info ||= Socket.unpack_sockaddr_in(get_peername)
    end

    def post_init
      @subscriptions = {}
      @verbose = @pedantic = true # suppressed by most clients, but allows friendly telnet
      @receive_data_calls = 0
      send_info
      @auth_pending = EM.add_timer(AUTH_TIMEOUT) { connect_auth_timeout } if Server.auth_required?
      debug "Client connection created", client_info
    end

    def connect_auth_timeout
      send_data AUTH_REQUIRED
      debug "Connection timeout due to lack of auth credentials"
      close_connection_after_writing
    end

    def receive_data(data)
      @receive_data_calls += 1
      (@buf ||= '') << data
      close_connection and return if @buf =~ /(\006|\004)/ # ctrl+c or ctrl+d for telnet friendly
      while (@buf && !@buf.empty?)
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
          trace 'PUB OP', op
          return if @auth_pending
          @pub_sub, @msg_size, @reply = $1, $2.to_i, $3
          send_data PAYLOAD_TOO_BIG and return if (@msg_size > MAX_PAYLOAD_SIZE)
          send_data INVALID_SUBJECT and return if @pedantic && !(@pub_sub =~ SUB_NO_WC)
        when SUB_OP
          trace 'SUB OP', op
          return if @auth_pending
          sub, sid = $1, $2          
          send_data INVALID_SUBJECT and return if !($1 =~ SUB)          
          send_data INVALID_SID_TAKEN and return if @subscriptions[sid]
          subscriber = Subscriber.new(self, sub, sid)
          @subscriptions[sid] = subscriber
          Server.subscribe(subscriber)
          send_data OK if @verbose
        when UNSUB_OP
          trace 'UNSUB OP', op
          return if @xsauth_pending
          sid, subscriber = $1, @subscriptions[$1]
          send_data INVALID_SID_NOEXIST and return unless subscriber
          Server.unsubscribe(subscriber)
          @subscriptions.delete(sid)
          send_data OK if @verbose
        when PING
          trace 'PING OP', op
          send_data PONG_RESPONSE
        when CONNECT
          trace 'CONNECT OP', op
          begin
            config = JSON.parse($1, :symbolize_keys => true)
            process_connect_config(config)
          rescue => e
            send_data INVALID_CONFIG
            log_error
          end
        when INFO
          trace 'INFO OP', op
          send_info
        else
          trace 'Unknown Op', op
          send_data "ERR 'Unkown Op'#{CR_LF}"
      end
    end

    def send_info
      send_data "INFO #{Server.info_string}#{CR_LF}"          
    end

    def process_msg(body)
      trace 'Processing msg', @pub_sub, body
      send_data OK if @verbose
      Server.route_to_subscribers(@pub_sub, body)
      @pub_sub = @msg_size = @payload_regex = nil
      true
    end
    
    def process_connect_config(config)
      @verbose  = config[:verbose] if config[:verbose]
      @pedantic = config[:pedantic] if config[:pedantic]

      send_data OK and return unless Server.auth_required?

      EM.cancel_timer(@auth_pending)
      if Server.auth_ok?(config[:user], config[:pass])
        send_data OK
        @auth_pending = nil
      else
        send_data AUTH_FAILED
        close_connection_after_writing
        debug "Authorization failed for connection"
      end
    end
    
    def unbind
      debug "Client connection closed", client_info
      trace "Receive_Data called #{@receive_data_calls} times."
      @subscriptions.each_value { |subscriber| Server.unsubscribe(subscriber) }
      EM.cancel_timer(@auth_pending) if @auth_pending
      @auth_pending = nil
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
  exit
end

trap("TERM") { shutdown }
trap("INT")  { shutdown }
 
EM.run {

  NATS::Server.setup(ARGV.dup)

  log "Starting nats server on port #{NATS::Server.port}"

  begin
    EM.set_descriptor_table_size(32768) # Requires Root privileges    
    EventMachine::start_server(NATS::Server.host, NATS::Server.port, NATS::Connection)
  rescue => e
    log "Could not start server on port #{NATS::Server.port}"
    log_error
    exit
  end

}
