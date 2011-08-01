module NATSD #:nodoc: all

  # Subscriber
  Subscriber = Struct.new(:conn, :subject, :sid, :qgroup, :num_responses, :max_responses)

  class Server

    class << self
      attr_reader :id, :info, :log_time, :auth_required, :debug_flag, :trace_flag, :options
      attr_reader :max_payload, :max_pending, :max_control_line, :auth_timeout
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
          unless @options[:log_file]
            # These log messages visible to controlling TTY
            log "Starting #{NATSD::APP_NAME} version #{NATSD::VERSION} on port #{NATSD::Server.port}"
            log "Starting http monitor on port #{@options[:http_port]}" if @options[:http_port]
            log "Switching to daemon mode"
          end
          Daemons.daemonize(:app_name => APP_NAME, :mode => :exec)
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
        user == @options[:user] && pass == @options[:pass]
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

        @healthz = 'ok\n'

        @varz = {
          :start => Time.now,
          :options => @options,
          :cores => num_cpu_cores
        }

        http_server = Thin::Server.new(NATSD::Server.host, port, :signals => false) do
          Thin::Logging.silent = true
          #use Rack::Auth::Basic do |username, password|
          #  [username, password] == auth
          #end
          map '/healthz' do
            run lambda { |env| [200, RACK_TEXT_HDR, NATSD::Server.healthz] }
          end
          map '/varz' do
            run Varz.new
          end
        end
        http_server.start!
      end

    end
  end

end
