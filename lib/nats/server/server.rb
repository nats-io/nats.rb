module NATSD #:nodoc: all

  # Subscriber
  Subscriber = Struct.new(:conn, :subject, :sid, :qgroup, :num_responses, :max_responses)

  class Server

    class << self
      attr_reader :id, :info, :log_time, :auth_required, :debug_flag, :trace_flag, :options
      attr_reader :max_payload, :max_pending, :max_control_line, :auth_timeout, :ping_interval, :ping_max
      attr_accessor :varz, :healthz, :num_connections, :in_msgs, :out_msgs, :in_bytes, :out_bytes

      alias auth_required? :auth_required
      alias debug_flag?    :debug_flag
      alias trace_flag?    :trace_flag

      def version; "nats-server version #{NATSD::VERSION}" end

      def host; @options[:addr] end
      def port; @options[:port] end
      def pid_file; @options[:pid_file] end

      def process_options(argv=[])
        @options = {}

        # Allow command line to override config file, so do them first.
        parser.parse!(argv)
        read_config_file if @options[:config_file]
        finalize_options
      rescue OptionParser::InvalidOption => e
        log_error "Error parsing options: #{e}"
        exit(1)
      end

      def setup(argv)
        process_options(argv)

        @id, @cid = fast_uuid, 1
        @sublist = Sublist.new

        @num_connections = 0
        @in_msgs = @out_msgs = 0
        @in_bytes = @out_bytes = 0

        @info = {
          :server_id => Server.id,
          :version => VERSION,
          :auth_required => auth_required?,
          :max_payload => @max_payload
        }

        # Check for daemon flag
        if @options[:daemonize]
          require 'rubygems'
          require 'daemons'
          require 'tmpdir'
          unless @options[:log_file]
            # These log messages visible to controlling TTY
            log "Starting #{NATSD::APP_NAME} version #{NATSD::VERSION} on port #{NATSD::Server.port}"
            log "Starting http monitor on port #{@options[:http_port]}" if @options[:http_port]
            log "Switching to daemon mode"
          end
          opts = {
            :app_name => APP_NAME,
            :mode => :exec,
            :dir_mode => :normal,
            :dir => Dir.tmpdir
          }
          Daemons.daemonize(opts)
          FileUtils.rm_f("#{Dir.tmpdir}/#{APP_NAME}.pid")
        end

        setup_logs

        # Setup optimized select versions
        EM.epoll unless @options[:noepoll]
        EM.kqueue unless @options[:nokqueue]

        # Write pid file if requested.
        File.open(@options[:pid_file], 'w') { |f| f.puts "#{Process.pid}" } if @options[:pid_file]
      end

      def subscribe(sub)
        @sublist.insert(sub.subject, sub)
      end

      def unsubscribe(sub)
        @sublist.remove(sub.subject, sub)
      end

      def deliver_to_subscriber(sub, subject, reply, msg)
        conn = sub.conn

        # Accounting
        @out_msgs += 1
        @out_bytes += msg.bytesize unless msg.nil?

        conn.send_data("MSG #{subject} #{sub.sid} #{reply}#{msg.bytesize}#{CR_LF}#{msg}#{CR_LF}")

        # Account for these response and check for auto-unsubscribe (pruning interest graph)
        sub.num_responses += 1
        conn.delete_subscriber(sub) if (sub.max_responses && sub.num_responses >= sub.max_responses)

        # Check the outbound queue here and react if need be..
        if conn.get_outbound_data_size > NATSD::Server.max_pending
          conn.error_close SLOW_CONSUMER
          log "Slow consumer dropped, exceeded #{NATSD::Server.max_pending} bytes pending", conn.client_info
        end
      end

      def route_to_subscribers(subject, reply, msg)
        qsubs = nil

        # Allows nil reply to not have extra space
        reply = reply + ' ' if reply

        # Accounting
        @in_msgs += 1
        @in_bytes += msg.bytesize unless msg.nil?

        @sublist.match(subject).each do |sub|
          # Skip anyone in the closing state
          next if sub.conn.closing

          unless sub[:qgroup]
            deliver_to_subscriber(sub, subject, reply, msg)
          else
            if NATSD::Server.trace_flag?
              trace("Matched queue subscriber", sub[:subject], sub[:qgroup], sub[:sid], sub.conn.client_info)
            end
            # Queue this for post processing
            qsubs ||= Hash.new
            qsubs[sub[:qgroup]] ||= []
            qsubs[sub[:qgroup]] << sub
          end
        end

        return unless qsubs

        qsubs.each_value do |subs|
          # Randomly pick a subscriber from the group
          sub = subs[rand*subs.size]
          if NATSD::Server.trace_flag?
            trace("Selected queue subscriber", sub[:subject], sub[:qgroup], sub[:sid], sub.conn.client_info)
          end
          deliver_to_subscriber(sub, subject, reply, msg)
        end
      end

      def auth_ok?(user, pass)
        @options[:users].each { |u| return true if (user == u[:user] && pass == u[:pass]) }
        false
      end

      def cid
        @cid += 1
      end

      def info_string
        @info.to_json
      end

      # Monitoring
      def start_http_server
        return unless port = @options[:http_port]

        require 'thin'

        log "Starting http monitor on port #{port}"

        @healthz = "ok\n"

        @varz = {
          :start => Time.now,
          :options => @options,
          :cores => num_cpu_cores
        }

        http_server = Thin::Server.new(@options[:http_net], port, :signals => false) do
          Thin::Logging.silent = true
          if NATSD::Server.options[:http_user]
            auth = [NATSD::Server.options[:http_user], NATSD::Server.options[:http_password]]
            use Rack::Auth::Basic do |username, password|
              [username, password] == auth
            end
          end
          map '/healthz' do
            run lambda { |env| [200, RACK_TEXT_HDR, NATSD::Server.healthz] }
          end
          map '/varz' do
            run Varz.new
          end
          map '/connz' do
            run Connz.new
          end
        end
        http_server.start!
      end

    end
  end

  module Connection #:nodoc: all

    attr_reader :cid, :closing
    alias :closing? :closing

    def client_info
      @client_info ||= Socket.unpack_sockaddr_in(get_peername)
    end

    def info
      {
        :cid => cid,
        :ip => client_info[1],
        :port => client_info[0],
        :subscriptions => @subscriptions.size,
        :pending_size => get_outbound_data_size
      }
    end

    def post_init
      @cid = Server.cid
      @subscriptions = {}
      @verbose = @pedantic = true # suppressed by most clients, but allows friendly telnet
      # @receive_data_calls = 0
      @parse_state = AWAITING_CONTROL_LINE
      send_info
      @auth_pending = EM.add_timer(NATSD::Server.auth_timeout) { connect_auth_timeout } if Server.auth_required?
      @ping_timer = EM.add_periodic_timer(NATSD::Server.ping_interval) { send_ping }
      @pings_outstanding = 0
      Server.num_connections += 1
      debug "Client connection created", client_info, cid
    end

    def send_ping
      return if @closing
      if @pings_outstanding > NATSD::Server.ping_max
        error_close UNRESPONSIVE
        return
      end
      send_data(PING_RESPONSE)
      @pings_outstanding += 1
    end

    def connect_auth_timeout
      error_close AUTH_REQUIRED
      debug "Connection timeout due to lack of auth credentials", cid
    end

    def receive_data(data)
      # @receive_data_calls += 1
      @buf = @buf ? @buf << data : data
      return close_connection if @buf =~ /(\006|\004)/ # ctrl+c or ctrl+d for telnet friendly

      # while (@buf && !@buf.empty? && !@closing)
      while (@buf && !@closing)
        case @parse_state
        when AWAITING_CONTROL_LINE
          case @buf
          when PUB_OP
            ctrace('PUB OP', strip_op($&)) if NATSD::Server.trace_flag?
            return connect_auth_timeout if @auth_pending
            @buf = $'
            @parse_state = AWAITING_MSG_PAYLOAD
            @msg_sub, @msg_reply, @msg_size = $1, $3, $4.to_i
            if (@msg_size > NATSD::Server.max_payload)
              debug "Message payload size exceeded (#{@msg_size}/#{NATSD::Server.max_payload}), closing connection"
              error_close PAYLOAD_TOO_BIG
            end
            send_data(INVALID_SUBJECT) if (@pedantic && !(@msg_sub =~ SUB_NO_WC))
          when SUB_OP
            ctrace('SUB OP', strip_op($&)) if NATSD::Server.trace_flag?
            return connect_auth_timeout if @auth_pending
            @buf = $'
            sub, qgroup, sid = $1, $3, $4
            return send_data(INVALID_SUBJECT) if !($1 =~ SUB)
            return send_data(INVALID_SID_TAKEN) if @subscriptions[sid]
            sub = Subscriber.new(self, sub, sid, qgroup, 0)
            @subscriptions[sid] = sub
            Server.subscribe(sub)
            send_data(OK) if @verbose
          when UNSUB_OP
            ctrace('UNSUB OP', strip_op($&)) if NATSD::Server.trace_flag?
            return connect_auth_timeout if @auth_pending
            @buf = $'
            sid, sub = $1, @subscriptions[$1]
            if sub
              # If we have set max_responses, we will unsubscribe once we have received
              # the appropriate amount of responses.
              sub.max_responses = ($2 && $3) ? $3.to_i : nil
              delete_subscriber(sub) unless (sub.max_responses && (sub.num_responses < sub.max_responses))
              send_data(OK) if @verbose
            else
              send_data(INVALID_SID_NOEXIST) if @pedantic
            end
          when PING
            ctrace('PING OP', strip_op($&)) if NATSD::Server.trace_flag?
            @buf = $'
            send_data(PONG_RESPONSE)
          when PONG
            ctrace('PONG OP', strip_op($&)) if NATSD::Server.trace_flag?
            @buf = $'
            @pings_outstanding -= 1
          when CONNECT
            ctrace('CONNECT OP', strip_op($&)) if NATSD::Server.trace_flag?
            @buf = $'
            begin
              config = JSON.parse($1)
              process_connect_config(config)
            rescue => e
              send_data(INVALID_CONFIG)
              log_error
            end
          when INFO
            ctrace('INFO OP', strip_op($&)) if NATSD::Server.trace_flag?
            return connect_auth_timeout if @auth_pending
            @buf = $'
            send_info
          when UNKNOWN
            ctrace('Unknown Op', strip_op($&)) if NATSD::Server.trace_flag?
            return connect_auth_timeout if @auth_pending
            @buf = $'
            send_data(UNKNOWN_OP)
          else
            # If we are here we do not have a complete line yet that we understand.
            # If too big, cut the connection off.
            if @buf.bytesize > NATSD::Server.max_control_line
              debug "Control line size exceeded (#{@buf.bytesize}/#{NATSD::Server.max_control_line}), closing connection.."
              error_close PROTOCOL_OP_TOO_BIG
            end
            return
          end
          @buf = nil if (@buf && @buf.empty?)

        when AWAITING_MSG_PAYLOAD
          return unless (@buf.bytesize >= (@msg_size + CR_LF_SIZE))
          msg = @buf.slice(0, @msg_size)
          ctrace('Processing msg', @msg_sub, @msg_reply, msg) if NATSD::Server.trace_flag?
          send_data(OK) if @verbose
          Server.route_to_subscribers(@msg_sub, @msg_reply, msg)
          @buf = @buf.slice((@msg_size + CR_LF_SIZE), @buf.bytesize)
          @msg_sub = @msg_size = @reply = nil
          @parse_state = AWAITING_CONTROL_LINE
          @buf = nil if (@buf && @buf.empty?)
        end
      end
    end

    def send_info
      send_data("INFO #{Server.info_string}#{CR_LF}")
    end

    def process_connect_config(config)
      @verbose  = config['verbose'] unless config['verbose'].nil?
      @pedantic = config['pedantic'] unless config['pedantic'].nil?
      return send_data(OK) unless Server.auth_required?

      EM.cancel_timer(@auth_pending)
      if Server.auth_ok?(config['user'], config['pass'])
        send_data(OK) if @verbose
        @auth_pending = nil
      else
        error_close AUTH_FAILED
        debug "Authorization failed for connection", cid
      end
    end

    def delete_subscriber(sub)
      ctrace('DELSUB OP', sub.subject, sub.qgroup, sub.sid) if NATSD::Server.trace_flag?
      Server.unsubscribe(sub)
      @subscriptions.delete(sub.sid)
    end

    def error_close(msg)
      send_data(msg)
      close_connection_after_writing
      @closing = true
    end

    def unbind
      debug "Client connection closed", client_info, cid
      Server.num_connections -= 1
      # ctrace "Receive_Data called #{@receive_data_calls} times." if @receive_data_calls > 0
      @subscriptions.each_value { |sub| Server.unsubscribe(sub) }
      EM.cancel_timer(@auth_pending) if @auth_pending
      @auth_pending = nil
      EM.cancel_timer(@ping_timer) if @ping_timer
      @ping_timer = nil

      @closing = true
    end

    def ctrace(*args)
      trace(args, "c: #{cid}")
    end

    def strip_op(op='')
      op.dup.sub(CR_LF, EMPTY)
    end
  end

end
