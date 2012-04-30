
$:.unshift('./lib')
require 'nats/client'

def timeout_nats_on_failure(to=0.25)
  EM.add_timer(to) { NATS.stop }
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

class NatsServerControl

  attr_reader :was_running
  alias :was_running? :was_running

  class << self
    def kill_autostart_server
      pid ||= File.read(NATS::AUTOSTART_PID_FILE).chomp.to_i
      %x[kill -9 #{pid}] if pid
      %x[rm #{NATS::AUTOSTART_PID_FILE}]
      %x[rm #{NATS::AUTOSTART_LOG_FILE}]
    end
  end

  def initialize(uri='nats://localhost:4222', pid_file='/tmp/test-nats.pid', flags=nil)
    @uri = URI.parse(uri)
    @pid_file = pid_file
    @flags = flags
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

    # daemonize really doesn't work on jruby, so should run servers manually to test on jruby
    args = "-p #{@uri.port} -P #{@pid_file}"
    args += " --user #{@uri.user}" if @uri.user
    args += " --pass #{@uri.password}" if @uri.password
    args += " #{@flags}" if @flags
    args += ' -d'
    %x[bundle exec nats-server #{args} 2> /dev/null]
    exitstatus = $?.exitstatus
    NATS.wait_for_server(@uri, 10) if wait_for_server #jruby can be slow on startup
    exitstatus
  end

  def kill_server
    if File.exists? @pid_file
      %x[kill -9 #{server_pid} 2> /dev/null]
      %x[rm #{@pid_file} 2> /dev/null]
      %x[rm #{NATS::AUTOSTART_LOG_FILE} 2> /dev/null]
      @pid = nil
    end
  end
end

module EchoServer

  HOST = "localhost".freeze
  PORT = "9999".freeze
  ECHO_SERVER = "http://#{HOST}:#{PORT}".freeze

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
