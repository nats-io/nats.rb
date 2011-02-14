
require 'socket'
require 'fileutils'
require 'pp'

ep = File.expand_path(File.dirname(__FILE__))

require "#{ep}/ext/em"
require "#{ep}/ext/bytesize"
require "#{ep}/ext/json"
require "#{ep}/server/sublist"
require "#{ep}/server/options"
require "#{ep}/server/const"

module NATSD #:nodoc: all

  # Subscriber
  Subscriber = Struct.new(:conn, :subject, :sid, :qgroup, :num_responses, :max_responses)

  class Server

    class << self
      attr_reader :id, :info, :log_time, :auth_required, :debug_flag, :trace_flag

      alias auth_required? :auth_required
      alias debug_flag?    :debug_flag
      alias trace_flag?    :trace_flag

      def version; "nats server version #{NATSD::VERSION}" end

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
          :server_id => Server.id,
          :version => VERSION,
          :auth_required => auth_required?,
          :max_payload => MAX_PAYLOAD_SIZE
        }

        # Check for daemon flag
        if @options[:daemonize]
          require 'rubygems'
          require 'daemons'
          # These log messages visible to controlling TTY
          log "Starting #{NATSD::APP_NAME} version #{NATSD::VERSION} on port #{NATSD::Server.port}"
          log "Switching to daemon mode"
          Daemons.daemonize(:app_name => APP_NAME, :mode => :exec)
        end

        setup_logs

        # Setup optimized select versions
        EM.epoll unless @options[:noepoll]
        EM.kqueue unless @options[:nokqueue]

        # Write pid file if need be.
        File.open(@options[:pid_file], 'w') { |f| f.puts "#{Process.pid}" } if @options[:pid_file]
      end

      def subscribe(sub)
        @sublist.insert(sub.subject, sub)
      end

      def unsubscribe(sub)
        @sublist.remove(sub.subject, sub)
      end

      def deliver_to_subscriber(sub, subject, reply, msg)

        # Allows nil reply to not have extra space
        reply = reply + ' ' if reply

        conn = sub.conn
        
        conn.send_data("MSG #{subject} #{sub.sid} #{reply}#{msg.bytesize}#{CR_LF}")
        conn.send_data(msg)
        conn.send_data(CR_LF)

        # Account for the response and check for auto-unsubscribe (pruning interest graph)
        sub.num_responses += 1
        conn.delete_subscriber(sub) if (sub.max_responses && sub.num_responses >= sub.max_responses)

        # Check the outbound queue here and react if need be..
        if conn.get_outbound_data_size > MAX_OUTBOUND_SIZE
          conn.error_close SLOW_CONSUMER
          log "Slow consumer dropped", conn.client_info
        end
      end

      def route_to_subscribers(subject, reply, msg)
        qsubs = nil

        @sublist.match(subject).each do |sub|
          # Skip anyone in the closing state
          next if sub.conn.closing

          unless sub[:qgroup]
            deliver_to_subscriber(sub, subject, reply, msg)
          else
            trace "Matched queue subscriber", sub[:subject], sub[:qgroup], sub[:sid], sub.conn.client_info
            # Queue this for post processing
            qsubs ||= Hash.new([])
            qsubs[sub[:qgroup]] = qsubs[sub[:qgroup]] << sub
          end
        end

        return unless qsubs

        qsubs.each_value do |subs|
          # Randomly pick a subscriber from the group
          sub = subs[rand*subs.size]
          trace "Selected queue subscriber", sub[:subject], sub[:qgroup], sub[:sid], sub.conn.client_info
          deliver_to_subscriber(sub, subject, reply, msg)
        end

      end

      def auth_ok?(user, pass)
        user == @options[:user] && pass == @options[:pass]
      end

      def cid
        @cid += 1
      end

      def info_string
        @info.to_json
      end

    end
  end

  module Connection #:nodoc:

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
      return close_connection if @buf =~ /(\006|\004)/ # ctrl+c or ctrl+d for telnet friendly
      while (@buf && !@buf.empty? && !@closing)
        # Waiting on msg payload
        if @msg_size
          if (@buf.bytesize >= (@msg_size + CR_LF_SIZE))
              msg = @buf.slice(0, @msg_size)
              process_msg(msg)
              @buf = @buf.slice((msg.bytesize + CR_LF_SIZE), @buf.bytesize)
          else # Waiting for additional msg data
            return
          end
        # Waiting on control line
        elsif @buf =~ /^(.*)\r\n/
          @buf = $'
          process_op($1)
        else # Waiting for additional data for control line
          # This is not normal. Close immediately
          if @buf.bytesize > MAX_CONTROL_LINE_SIZE
            debug "MAX_CONTROL_LINE exceeded, closing connection.."
            @closing = true
            close_connection
          end
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
          return send_data PAYLOAD_TOO_BIG if (@msg_size > MAX_PAYLOAD_SIZE)
          return send_data INVALID_SUBJECT if (@pedantic && !(@pub_sub =~ SUB_NO_WC))
        when SUB_OP
          ctrace 'SUB OP', op
          return if @auth_pending
          sub, qgroup, sid = $1, $3, $4
          return send_data INVALID_SUBJECT if !($1 =~ SUB)
          return send_data INVALID_SID_TAKEN if @subscriptions[sid]
          sub = Subscriber.new(self, sub, sid, qgroup, 0)
          @subscriptions[sid] = sub
          Server.subscribe(sub)
          send_data OK if @verbose
        when UNSUB_OP
          ctrace 'UNSUB OP', op
          return if @auth_pending
          sid, sub = $1, @subscriptions[$1]
          return send_data INVALID_SID_NOEXIST unless sub

          # If we have set max_responses, we will unsubscribe once we have received the appropriate
          # amount of responses
          sub.max_responses = ($2 && $3) ? $3.to_i : nil
          delete_subscriber(sub) unless (sub.max_responses && (sub.num_responses < sub.max_responses))
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

      return send_data OK unless Server.auth_required?

      EM.cancel_timer(@auth_pending)
      if Server.auth_ok?(config[:user], config[:pass])
        send_data OK
        @auth_pending = nil
      else
        error_close AUTH_FAILED
        debug "Authorization failed for connection", cid
      end
    end

    def delete_subscriber(sub)
      ctrace 'DELSUB OP', sub.subject, sub.qgroup, sub.sid
      Server.unsubscribe(sub)
      @subscriptions.delete(sub.sid)
    end

    def error_close(msg)
      send_data msg
      close_connection_after_writing
      @closing = true
    end

    def unbind
      debug "Client connection closed", client_info, cid
      ctrace "Receive_Data called #{@receive_data_calls} times." if @receive_data_calls > 0
      @subscriptions.each_value { |sub| Server.unsubscribe(sub) }
      EM.cancel_timer(@auth_pending) if @auth_pending
      @auth_pending = nil
    end

    def ctrace(*args)
      trace(args, "c: #{cid}")
    end
  end

end

def fast_uuid #:nodoc:
  v = [rand(0x0010000),rand(0x0010000),rand(0x0010000),
       rand(0x0010000),rand(0x0010000),rand(0x1000000)]
  "%04x%04x%04x%04x%04x%06x" % v
end

def log(*args) #:nodoc:
  args.unshift(Time.now) if NATSD::Server.log_time
  pp args.compact
end

def debug(*args) #:nodoc:
  log *args if NATSD::Server.debug_flag?
end

def trace(*args) #:nodoc:
  log *args if NATSD::Server.trace_flag?
end

def log_error(e=$!) #:nodoc:
  debug e, e.backtrace
end

def shutdown #:nodoc:
  puts
  log 'Server exiting..'
  EM.stop
  FileUtils.rm(NATSD::Server.pid_file) if NATSD::Server.pid_file
  exit
end

['TERM','INT'].each { |s| trap(s) { shutdown } }

# Do setup
NATSD::Server.setup(ARGV.dup)

# Event Loop

EM.run {

  log "Starting #{NATSD::APP_NAME} version #{NATSD::VERSION} on port #{NATSD::Server.port}"

  begin
    EM.set_descriptor_table_size(32768) # Requires Root privileges
    EventMachine::start_server(NATSD::Server.host, NATSD::Server.port, NATSD::Connection)
  rescue => e
    log "Could not start server on port #{NATSD::Server.port}"
    log_error
    exit
  end

}
