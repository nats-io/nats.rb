require 'set'

module NATSD #:nodoc: all

  # Subscriber
  Subscriber = Struct.new(:conn, :subject, :sid, :qgroup, :num_responses, :max_responses)

  class Server

    class << self
      attr_reader :id, :info, :log_time, :auth_required, :ssl_required, :debug_flag, :trace_flag, :syslog, :options
      attr_reader :max_payload, :max_pending, :max_control_line, :auth_timeout, :ssl_timeout, :ping_interval, :ping_max
      attr_accessor :varz, :healthz, :connections, :max_connections, :num_connections, :in_msgs, :out_msgs, :in_bytes, :out_bytes

      alias auth_required? :auth_required
      alias ssl_required?  :ssl_required
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

        @id, @cid, @rid = fast_uuid, 1, 1
        @sublist = Sublist.new

        @connections = {}
        @num_connections = 0
        @in_msgs = @out_msgs = 0
        @in_bytes = @out_bytes = 0

        @num_routes = 0

        @info = {
          :server_id => Server.id,
          :host => host,
          :port => port,
          :version => VERSION,
          :auth_required => auth_required?,
          :ssl_required => ssl_required?,
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
            log "Starting routing on port #{@options[:cluster_port]}" if @options[:cluster_port]
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
        open_syslog

        # Setup optimized select versions
        EM.epoll unless @options[:noepoll]
        EM.kqueue unless @options[:nokqueue]

        # Write pid file if requested.
        File.open(@options[:pid_file], 'w') { |f| f.puts "#{Process.pid}" } if @options[:pid_file]
      end

      def subscribe(sub, is_route=false)
        @sublist.insert(sub.subject, sub)
        broadcast_sub_to_routes(sub) unless is_route
      end

      def unsubscribe(sub, is_route=false)
        @sublist.remove(sub.subject, sub)
        broadcast_unsub_to_routes(sub) unless is_route
      end

      def deliver_to_subscriber(sub, subject, reply, msg)
        conn = sub.conn

        # Accounting
        @out_msgs += 1
        conn.out_msgs += 1
        unless msg.nil?
          mbs = msg.bytesize
          @out_bytes += mbs
          conn.out_bytes += mbs
        end

        conn.queue_data("MSG #{subject} #{sub.sid} #{reply}#{msg.bytesize}#{CR_LF}#{msg}#{CR_LF}")

        # Account for these response and check for auto-unsubscribe (pruning interest graph)
        sub.num_responses += 1
        conn.delete_subscriber(sub) if (sub.max_responses && sub.num_responses >= sub.max_responses)

        # Check the outbound queue here and react if need be..
        if (conn.get_outbound_data_size + conn.writev_size) > NATSD::Server.max_pending
          conn.error_close SLOW_CONSUMER
          maxp = pretty_size(NATSD::Server.max_pending)
          log "Slow consumer dropped, exceeded #{maxp} pending", conn.client_info
        end
      end

      def route_to_subscribers(subject, reply, msg, is_route=false)
        qsubs = nil

        # Allows nil reply to not have extra space
        reply = reply + ' ' if reply

        # Accounting
        @in_msgs += 1
        @in_bytes += msg.bytesize unless msg.nil?

        # Routes
        routes = nil

        @sublist.match(subject).each do |sub|
          # Skip anyone in the closing state
          next if sub.conn.closing

          # Skip all routes if sourced from another route (1-hop semantics)
          next if (is_route && sub.conn.is_route?)

          if sub[:qgroup].nil?
            if sub.conn.is_route?
              # Only send messages once over a given route
              routes ||= Set.new
              deliver_to_subscriber(sub, subject, reply, msg) unless routes.include?(sub.conn.remote_rid)
              routes << sub.conn.remote_rid
            else
              deliver_to_subscriber(sub, subject, reply, msg)
            end
          elsif !is_route
            if NATSD::Server.trace_flag?
              trace('Matched queue subscriber', sub[:subject], sub[:qgroup], sub[:sid], sub.conn.client_info)
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
            trace('Selected queue subscriber', sub[:subject], sub[:qgroup], sub[:sid], sub.conn.client_info)
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

      def rid
        @rid += 1
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

end
