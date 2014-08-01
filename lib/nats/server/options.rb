require 'optparse'
require 'yaml'

module NATSD
  class Server

    class << self
      def parser
        @parser ||= OptionParser.new do |opts|
          opts.banner = "Usage: nats-server [options]"

          opts.separator ""
          opts.separator "Server options:"

          opts.on("-a", "--addr HOST", "Bind to HOST address " +
                                       "(default: #{DEFAULT_HOST})")           { |host| @options[:addr] = host }
          opts.on("-p", "--port PORT", "Use PORT (default: #{DEFAULT_PORT})")  { |port| @options[:port] = port.to_i }
          opts.on("-d", "--daemonize", "Run daemonized in the background")     { @options[:daemonize] = true }
          opts.on("-P", "--pid FILE",  "File to store PID")                    { |file| @options[:pid_file] = file }

          opts.on("-m", "--http_port PORT", "Use HTTP PORT ")                  { |port| @options[:http_port] = port.to_i }

          opts.on("-r", "--cluster_port PORT", "Use Cluster PORT ")            { |port| @options[:cluster_port] = port.to_i }

          opts.on("-c", "--config FILE", "Configuration File")                 { |file| @options[:config_file] = file }

          opts.separator ""
          opts.separator "Logging options:"

          opts.on("-l", "--log FILE", "File to redirect log output")           { |file| @options[:log_file] = file }
          opts.on("-T", "--logtime", "Timestamp log entries (default: false)") { @options[:log_time] = true }
          opts.on("-S", "--syslog IDENT", "Enable Syslog output")              { |ident| @options[:syslog] = ident }
          opts.on("-D", "--debug", "Enable debugging output")                  { @options[:debug] = true }
          opts.on("-V", "--trace", "Trace the raw protocol")                   { @options[:trace] = true }

          opts.separator ""
          opts.separator "Authorization options:"

          opts.on("--user user", "User required for connections")              { |user| @options[:user] = user }
          opts.on("--pass password", "Password required for connections")      { |pass| @options[:pass] = pass }

          opts.separator ""
          opts.on("--ssl", "Enable SSL")                                       { |ssl| @options[:ssl] = true }

          opts.separator ""
          opts.separator "Advanced IO options:"

          opts.on("--no_epoll", "Disable epoll (Linux)")                       { @options[:noepoll] = true }
          opts.on("--no_kqueue", "Disable kqueue (MacOSX and BSD)")            { @options[:nokqueue] = true }

          opts.separator ""
          opts.separator "Common options:"

          opts.on_tail("-h", "--help", "Show this message")                    { puts opts; exit }
          opts.on_tail('-v', '--version', "Show version")                      { puts NATSD::Server.version; exit }
        end
      end

      def read_config_file
        return unless config_file = @options[:config_file]
        config = File.open(config_file) { |f| YAML.load(f) }

        # Command lines args, parsed first, will override these.
        @options[:port] = config['port'] if @options[:port].nil?
        @options[:addr] = config['net'] if @options[:addr].nil?

        if auth = config['authorization']
          @options[:user] = auth['user'] if @options[:user].nil?
          @options[:pass] = auth['password'] if @options[:pass].nil?
          @options[:pass] = auth['pass'] if @options[:pass].nil?
          @options[:token] = auth['token'] if @options[:token].nil?
          @options[:auth_timeout] = auth['timeout'] if @options[:auth_timeout].nil?
          # Multiple Users setup
          @options[:users] = symbolize_users(auth['users']) || []
        end

        # TLS/SSL
        @options[:ssl] = config['ssl'] if @options[:ssl].nil?

        @options[:pid_file] = config['pid_file'] if @options[:pid_file].nil?
        @options[:log_file] = config['log_file'] if @options[:log_file].nil?
        @options[:log_time] = config['logtime'] if @options[:log_time].nil?
        @options[:syslog] = config['syslog'] if @options[:syslog].nil?
        @options[:debug] = config['debug'] if @options[:debug].nil?
        @options[:trace] = config['trace'] if @options[:trace].nil?

        # these just override if present
        @options[:max_control_line] = config['max_control_line'] if config['max_control_line']
        @options[:max_payload] = config['max_payload'] if config['max_payload']
        @options[:max_pending] = config['max_pending'] if config['max_pending']
        @options[:max_connections] = config['max_connections'] if config['max_connections']

        # just set
        @options[:noepoll]  = config['no_epoll'] if config['no_epoll']
        @options[:nokqueue] = config['no_kqueue'] if config['no_kqueue']

        if http = config['http']
          if @options[:http_net].nil?
            @options[:http_net] = http['net'] || @options[:addr]
          end
          @options[:http_port] = http['port'] if @options[:http_port].nil?
          @options[:http_user] = http['user'] if @options[:http_user].nil?
          @options[:http_password] = http['password'] if @options[:http_password].nil?
        end

        if ping = config['ping']
          @options[:ping_interval] = ping['interval'] if @options[:ping_interval].nil?
          @options[:ping_max] = ping['max_outstanding'] if @options[:ping_max].nil?
        end

        if cluster = config['cluster']
          @options[:cluster_port] = cluster['port'] if @options[:cluster_port].nil?
          if auth = cluster['authorization']
            @options[:cluster_user] = auth['user'] if @options[:cluster_user].nil?
            @options[:cluster_pass] = auth['password'] if @options[:cluster_pass].nil?
            @options[:cluster_pass] = auth['pass'] if @options[:cluster_pass].nil?
            @options[:cluster_token] = auth['token'] if @options[:cluster_token].nil?
            @options[:cluster_auth_timeout] = auth['timeout'] if @options[:cluster_auth_timeout].nil?
            @route_auth_required = true
          end
          if routes = cluster['routes']
            @options[:cluster_routes] = routes if @options[:cluster_routes].nil?
          end
        end

      rescue => e
        log "Could not read configuration file:  #{e}"
        exit 1
      end

      def setup_logs
        return unless @options[:log_file]
        $stdout.reopen(@options[:log_file], 'a')
        $stdout.sync = true
        $stderr.reopen($stdout)
      end

      def open_syslog
        return unless @options[:syslog]
        Syslog.open("#{@options[:syslog]}", Syslog::LOG_PID, Syslog::LOG_USER) unless Syslog.opened?
      end

      def close_syslog
        Syslog.close if @options[:syslog]
      end

      def symbolize_users(users)
        return nil unless users
        auth_users = []
        users.each do |u|
          auth_users << { :user => u['user'], :pass => u['pass'] || u['password'] }
        end
        auth_users
      end

      def finalize_options
        # Addr/Port
        @options[:port] ||= DEFAULT_PORT
        @options[:addr] ||= DEFAULT_HOST

        # Max Connections
        @options[:max_connections] ||= DEFAULT_MAX_CONNECTIONS
        @max_connections = @options[:max_connections]

        # Debug and Tracing
        @debug_flag = @options[:debug]
        @trace_flag = @options[:trace]

        # Log timestamps
        @log_time = @options[:log_time]

        debug @options # Block pass?
        debug "DEBUG is on"
        trace "TRACE is on"

        # Syslog
        @syslog = @options[:syslog]

        # Authorization

        # Multi-user setup for auth
        if @options[:user]
          # Multiple Users setup
          @options[:users] ||= []
          @options[:users].unshift({:user => @options[:user], :pass => @options[:pass]}) if @options[:user]
        elsif @options[:users]
          first = @options[:users].first
          @options[:user], @options[:pass] = first[:user], first[:pass]
        end

        @auth_required = (not @options[:user].nil?)

        @ssl_required = @options[:ssl]

        # Pings
        @options[:ping_interval] ||= DEFAULT_PING_INTERVAL
        @ping_interval = @options[:ping_interval]

        @options[:ping_max] ||= DEFAULT_PING_MAX
        @ping_max = @options[:ping_max]

        # Thresholds
        @options[:max_control_line] ||= MAX_CONTROL_LINE_SIZE
        @max_control_line = @options[:max_control_line]

        @options[:max_payload] ||= MAX_PAYLOAD_SIZE
        @max_payload = @options[:max_payload]

        @options[:max_pending] ||= MAX_PENDING_SIZE
        @max_pending = @options[:max_pending]

        @options[:auth_timeout] ||= AUTH_TIMEOUT
        @auth_timeout = @options[:auth_timeout]

        @options[:ssl_timeout] ||= SSL_TIMEOUT
        @ssl_timeout = @options[:ssl_timeout]
      end

    end
  end
end
