require 'spec_helper'
require 'yaml'

describe 'max connections support' do

  before (:all) do
    MC_SERVER_PID = '/tmp/nats_mc_pid.pid'
    MC_SERVER = 'nats://localhost:9272'
    MC_LOG_FILE = '/tmp/nats_mc_test.log'
    MC_CONFIG = File.dirname(__FILE__) + '/resources/max_connections.yml'
    MC_FLAGS = "-c #{MC_CONFIG}"

    FileUtils.rm_f(MC_LOG_FILE)
    @s = NatsServerControl.new(MC_SERVER, MC_SERVER_PID, MC_FLAGS)
    @s.start_server
  end

  after(:all) do
    @s.kill_server
    NATS.server_running?(MC_SERVER).should be_falsey
    FileUtils.rm_f(MC_LOG_FILE)
  end

  it 'should not allow connections above the maximum allowed' do
    config = File.open(MC_CONFIG) { |f| YAML.load(f) }
    max = config['max_connections']

    err_received = false
    conns = []

    EM.run do
      (1..max).each { conns << NATS.connect({:uri => MC_SERVER}) }
      wait_on_connections(conns) do
        c = NATS.connect({:uri => MC_SERVER})
        EM.add_timer(0.25) { NATS.stop }
        c.on_error do |err|
          err_received = true
          err.should be_an_instance_of NATS::ServerError
          c.close
          conns.each { |c| c.close }
          EM.stop
        end
      end
    end
    err_received.should be_truthy
  end

end
