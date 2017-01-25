$:.unshift('./lib')
require 'nats/io/client'
require 'tempfile'
require 'monitor'

class NatsServerControl

  attr_reader :was_running
  alias :was_running? :was_running

  class << self

    def init_with_config(config_file)
      config = File.open(config_file) { |f| YAML.load(f) }
      if auth = config['authorization']
        uri = "nats://#{auth['user']}:#{auth['password']}@#{config['net']}:#{config['port']}"
      else
        uri = "nats://#{config['net']}:#{config['port']}"
      end
      NatsServerControl.new(uri, config['pid_file'], "-c #{config_file}")
    end

    def init_with_config_from_string(config_string, config={})
      puts config_string if ENV["DEBUG_NATS_TEST"] == "true"
      config_file = Tempfile.new(['nats-cluster-tests', '.conf'])
      File.open(config_file.path, 'w') do |f|
        f.puts(config_string)
      end

      if auth = config['authorization']
        uri = "nats://#{auth['user']}:#{auth['password']}@#{config['host']}:#{config['port']}"
      else
        uri = "nats://#{config['host']}:#{config['port']}"
      end

      NatsServerControl.new(uri, config['pid_file'], "-c #{config_file.path}", config_file)
    end

  end

  attr_reader :uri

  def initialize(uri='nats://127.0.0.1:4222', pid_file='/tmp/test-nats.pid', flags=nil, config_file=nil)
    @uri = uri.is_a?(URI) ? uri : URI.parse(uri)
    @pid_file = pid_file
    @flags = flags
    @config_file = config_file
  end

  def server_pid
    @pid ||= File.read(@pid_file).chomp.to_i
  end

  def server_mem_mb
    server_status = %x[ps axo pid=,rss= | grep #{server_pid}]
    parts = server_status.lstrip.split(/\s+/)
    rss = (parts[1].to_i)/1024
  end

  def start_server(wait_for_server=true)
    if server_running? @uri
      @was_running = true
      return 0
    end
    @pid = nil

    args = "-p #{@uri.port} -P #{@pid_file}"

    if @uri.user && !@uri.password
      args += " --auth #{@uri.user}"
    else
      args += " --user #{@uri.user}" if @uri.user
      args += " --pass #{@uri.password}" if @uri.password
    end
    args += " #{@flags}" if @flags

    if ENV["DEBUG_NATS_TEST"] == "true"
      system("gnatsd #{args} -DV &")
    else
      system("gnatsd #{args} 2> /dev/null &")
    end
    exitstatus = $?.exitstatus
    wait_for_server(@uri, 10) if wait_for_server
    exitstatus
  end

  def kill_server
    if File.exists? @pid_file
      %x[kill -9 #{server_pid} 2> /dev/null]
      %x[rm #{@pid_file} 2> /dev/null]
      sleep(0.1)
      @pid = nil
    end
  end

  def wait_for_server(uri, max_wait = 5) # :nodoc:
    start = Time.now
    while (Time.now - start < max_wait) # Wait max_wait seconds max
      break if server_running?(uri)
      sleep(0.1)
    end
  end

  def server_running?(uri) # :nodoc:
    require 'socket'
    s = TCPSocket.new(uri.host, uri.port)
    s.close
    return true
  rescue
    return false
  end
end

def with_timeout(timeout)
  start_time = Time.now
  yield
  end_time = Time.now
  duration = end_time - start_time
  fail if end_time - start_time > timeout
end

class Stream < Queue
  include MonitorMixin
end
