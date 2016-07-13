
$:.unshift('./lib')
require 'nats/client'
require 'tempfile'

def timeout_nats_on_failure(to=0.25)
  EM.add_timer(to) do
    EM.stop
  end
end

def timeout_em_on_failure(to=0.25)
  EM.add_timer(to) do
    EM.stop
  end
end

def with_em_timeout(to=1)
  EM.run do
    t = EM.add_timer(to) do
      NATS.stop
      EM.stop if EM.reactor_running?
    end

    fib = Fiber.new do |nc|
      nc.close if nc
      EM.cancel_timer(t)
      EM.stop if EM.reactor_running?
    end

    yield fib
  end
end

def wait_on_connections(conns)
  return unless conns
  expected, ready = conns.size, 0
  conns.each do |c|
    c.flush do
      ready += 1
      yield if ready >= expected
    end
  end
end

def flush_routes(conns, &blk)
  wait_on_routes_connected(conns, &blk)
end

def wait_on_routes_connected(conns)
  return unless conns && conns.size > 1

  sub = NATS.create_inbox
  ready = Array.new(conns.size, false)
  yield_needed = true

  conns.each_with_index do |c, i|
    c.subscribe(sub) do |msg|
      ready[i] = true
      done, yn = ready.all?, yield_needed
      yield_needed = false if yn && done
      yield if yn && done
    end
  end
  conn = conns.first
  timer = EM.add_periodic_timer(0.1) do
    conn.publish(sub)
    timer.cancel if ready.all?
  end
end

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
    if NATS.server_running? @uri
      @was_running = true
      return 0
    end
    @pid = nil

    args = "-p #{@uri.port} -P #{@pid_file}"
    args += " --user #{@uri.user}" if @uri.user
    args += " --pass #{@uri.password}" if @uri.password
    args += " #{@flags}" if @flags

    if ENV["DEBUG_NATS_TEST"] == "true"
      system("gnatsd #{args} -DV &")
    else
      system("gnatsd #{args} 2> /dev/null &")
    end
    exitstatus = $?.exitstatus
    NATS.wait_for_server(@uri, 10) if wait_for_server
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

end

# For running tests against the Ruby NATS server :)
class RubyNatsServerControl < NatsServerControl
  def start_server(wait_for_server=true)
    if NATS.server_running? @uri
      @was_running = true
      return 0
    end

    @pid = nil

    # daemonize really doesn't work on jruby, so should run servers manually to test on jruby
    args = "-p #{@uri.port} -P #{@pid_file}"
    args += " --user #{@uri.user}" if @uri.user
    args += " --pass #{@uri.password}" if @uri.password
    args += " #{@flags}" if @flags
    args += ' -d'

    %x[bundle exec ruby ./bin/nats-server #{args} 2> /dev/null]
    exitstatus = $?.exitstatus
    NATS.wait_for_server(@uri, 10) if wait_for_server #jruby can be slow on startup
    exitstatus
  end
end

module EchoServer

  HOST = "127.0.0.1".freeze
  PORT = "9999".freeze
  URI  = "nats://#{HOST}:#{PORT}".freeze

  def receive_data(data)
    send_data(data)
  end

  class << self
    def start(&blk)
      EM.run {
        EventMachine::start_server(HOST, PORT, self)
        blk.call
      }
    end

    def stop
      EM.stop_event_loop
    end
  end
end

module SilentServer

  HOST = "127.0.0.1".freeze
  PORT = "9998".freeze
  URI  = "nats://#{HOST}:#{PORT}".freeze

  # Does not send anything back
  def receive_data(data)
  end

  class << self
    def start(&blk)
      EM.run {
        EventMachine::start_server(HOST, PORT, self)
        blk.call
      }
    end

    def stop
      EM.stop_event_loop
    end
  end
end
