require 'spec_helper'
require 'uri'
require 'yaml'

describe 'cluster_load_balance' do

  before(:all) do
    LB1_CONFIG_FILE = File.dirname(__FILE__) + '/resources/b1_cluster.yml'
    @s1 = NatsServerControl.init_with_config(LB1_CONFIG_FILE)
    @s1.start_server

    LB2_CONFIG_FILE = File.dirname(__FILE__) + '/resources/b2_cluster.yml'
    @s2 = NatsServerControl.init_with_config(LB2_CONFIG_FILE)
    @s2.start_server
  end

  after(:all) do
    @s1.kill_server
    @s2.kill_server
  end

  # Tests that the randomize pool works correctly.
  it 'should properly load balance between multiple servers with same client config' do
    clients = []
    servers = { @s1.uri => 0, @s2.uri => 0 }
    EM.run do
      for i in 0...20
        clients << NATS.connect(:uri => servers.keys)
      end
      results = {}
      wait_on_connections(clients) do
        clients.each do |c|
          servers[c.connected_server] += 1
          port, ip = Socket.unpack_sockaddr_in(c.get_peername)
          port.should == URI(c.connected_server).port
        end
        servers.each_value { |v| v.should > 0 }
        EM.stop
      end
    end
  end

end
