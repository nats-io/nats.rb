
require 'optparse'
require 'yaml'

module NATSD
  class Server

    class << self
      def parser
        @parser ||= OptionParser.new do |opts|
          opts.banner = "Usage: nats [options]"

          opts.separator ""
          opts.separator "Server options:"

          opts.on("-a", "--addr HOST", "Bind to HOST address " +
                                       "(default: #{@options[:addr]})")                 { |host| @options[:address] = host }
          opts.on("-p", "--port PORT", "Use PORT (default: #{@options[:port]})")        { |port| @options[:port] = port.to_i }

          opts.on("-d", "--daemonize", "Run daemonized in the background")              { @options[:daemonize] = true }
          opts.on("-l", "--log FILE", "File to redirect output " +
                                      "(default: #{@options[:log_file]})")              { |file| @options[:log_file] = file }
          opts.on("-T", "--logtime", "Timestamp log entries")                           { @options[:log_time] = true }

          opts.on("-P", "--pid FILE", "File to store PID " +
                                      "(default: #{@options[:pid_file]})")              { |file| @options[:pid_file] = file }

          opts.on("-C", "--config FILE", "Configuration File " +
                                      "(default: #{@options[:config_file]})")           { |file| @options[:config_file] = file }

          opts.separator ""
          opts.separator "Authorization options: (Should be done in config file for production)"

          opts.on("--user user", "User required for connections")                       { |user| @options[:user] = user }

          opts.on("--pass password", "Password required for connections")               { |pass| @options[:pass] = pass }
          opts.on("--password password", "Password required for connections")           { |pass| @options[:pass] = pass }

          opts.on("--no_epoll", "Enable epoll (Linux)")                                 { @options[:noepoll] = true }
          opts.on("--kqueue", "Enable kqueue (MacOSX and BSD)")                         { @options[:nokqueue] = true }

          opts.separator ""
          opts.separator "Common options:"

          opts.on_tail("-h", "--help", "Show this message")                             { puts opts; exit }
          opts.on_tail('-v', '--version', "Show version")                               { puts NATSD::Server.version; exit }
          opts.on_tail("-D", "--debug", "Set debugging on")                             { @options[:debug] = true }
          opts.on_tail("-V", "--trace", "Set tracing on of raw protocol")               { @options[:trace] = true }
        end
      end

      def read_config_file
        return unless config_file = @options[:config_file]
        config = File.open(config_file) { |f| YAML.load(f) }
        # Command lines args, parsed first, will override these.
        [:addr, :port, :log_file, :pid_file, :user, :pass, :log_time, :debug].each do |p|
          c = config[p.to_s]
          @options[p] = c if c and not @options[p]
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
        @options[:addr] ||= '0.0.0.0'

        # Debug and Tracing
        @debug_flag = @options[:debug]
        @trace_flag = @options[:trace]

        # Log timestamps
        @log_time = @options[:log_time]
        # setup_logs

        debug @options # Block pass?
        debug "DEBUG is on"
        trace "TRACE is on"

        # Auth
        @auth_required = (@options[:user] != nil)
      end

    end

  end
end
