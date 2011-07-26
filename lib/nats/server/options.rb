
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

          opts.on("-c", "--config FILE", "Configuration File")                 { |file| @options[:config_file] = file }

          opts.separator ""
          opts.separator "Logging options:"

          opts.on("-l", "--log FILE", "File to redirect log output")           { |file| @options[:log_file] = file }
          opts.on("-T", "--logtime", "Timestamp log entries (default: false)") { @options[:log_time] = true }
          opts.on("-D", "--debug", "Enable debugging output")                  { @options[:debug] = true }
          opts.on("-V", "--trace", "Trace the raw protocol")                   { @options[:trace] = true }

          opts.separator ""
          opts.separator "Authorization options:"

          opts.on("--user user", "User required for connections")              { |user| @options[:user] = user }
          opts.on("--pass password", "Password required for connections")      { |pass| @options[:pass] = pass }

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
        end

        @options[:pid_file] = config['pid_file'] if @options[:pid_file].nil?
        @options[:log_file] = config['log_file'] if @options[:log_file].nil?
        @options[:log_time] = config['logtime'] if @options[:log_time].nil?
        @options[:debug] = config['debug'] if @options[:debug].nil?
        @options[:trace] = config['trace'] if @options[:trace].nil?

        # these just override if present
        @options[:max_control_line] = config['max_control_line'] if config['max_control_line']
        @options[:max_payload] = config['max_payload'] if config['max_payload']
        @options[:max_pending] = config['max_pending'] if config['max_pending']

        # just set
        @options[:noepoll]  = config['no_epoll'] if config['no_epoll']
        @options[:nokqueue] = config['no_kqueue'] if config['no_kqueue']

        if http = config['http']
          @options[:http_port] = http['port'] if @options[:http_port].nil?
          @options[:http_user] = http['user'] if @options[:http_user].nil?
          @options[:http_password] = http['password'] if @options[:http_password].nil?
        end

      rescue => e
        log "Could not read configuration file:  #{e}"
        exit
      end

      def setup_logs
        return unless @options[:log_file]
        $stdout.reopen(@options[:log_file], "w")
        $stdout.sync = true
        $stderr.reopen($stdout)
      end

      def finalize_options
        # Addr/Port
        @options[:port] ||= DEFAULT_PORT
        @options[:addr] ||= DEFAULT_HOST

        # Debug and Tracing
        @debug_flag = @options[:debug]
        @trace_flag = @options[:trace]

        # Log timestamps
        @log_time = @options[:log_time]

        debug @options # Block pass?
        debug "DEBUG is on"
        trace "TRACE is on"

        # Authorization
        @auth_required = (not @options[:user].nil?)

        # Thresholds
        @options[:max_control_line] ||= MAX_CONTROL_LINE_SIZE
        @max_control_line = @options[:max_control_line]

        @options[:max_payload] ||= MAX_PAYLOAD_SIZE
        @max_payload = @options[:max_payload]

        @options[:max_pending] ||= MAX_PENDING_SIZE
        @max_pending = @options[:max_pending]

        @options[:auth_timeout] ||= AUTH_TIMEOUT
        @auth_timeout = @options[:auth_timeout]
      end

    end
  end
end
