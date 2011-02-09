
$:.unshift('./lib')
require 'nats/client'

def timeout_nats_on_failure(to=0.25)
  EM.add_timer(to) { NATS.stop }
end

class NatsServerControl

  def initialize(uri="nats://localhost:4222", pid_file='/tmp/nats.pid')
    @uri = URI.parse(uri)
    @pid_file = pid_file
  end

  def server_pid
    @pid ||= File.read(@pid_file).chomp.to_i
  end

  def server_mem_mb
    server_status = %x[ps axo pid=,rss= | grep #{server_pid}]
    parts = server_status.lstrip.split(/\s+/)
    rss = (parts[1].to_i)/1024
  end

  def start_server
    return if NATS.server_running? @uri
    # This should work but is sketchy and slow under jruby, so use direct
    # %x[ruby -S bundle exec nats-server -p #{@uri.port} -P #{@pid_file} -d 2> /dev/null]
    server = File.expand_path(File.join(__FILE__, "../../lib/nats/server.rb"))
    # daemonize really doesn't work on jruby, so should run servers manually to test on jruby
    %x[ruby #{server} -p #{@uri.port} -P #{@pid_file} -d 2> /dev/null]
    NATS.wait_for_server(@uri, 10) #jruby can be slow on startup
  end

  def kill_server
    if File.exists? @pid_file
      %x[kill -9 #{server_pid}]
      %x[rm #{@pid_file}]
    end
  end
end
