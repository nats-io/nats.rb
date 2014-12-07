require 'spec_helper'
require 'yaml'

describe 'cluster retry connect' do

  before(:all) do
    SR1_CONFIG_FILE = File.dirname(__FILE__) + '/resources/s1_cluster.yml'
    @s1 = NatsServerControl.init_with_config(SR1_CONFIG_FILE)
    @s1.start_server

    SR2_CONFIG_FILE = File.dirname(__FILE__) + '/resources/s2_cluster.yml'
    @s2 = NatsServerControl.init_with_config(SR2_CONFIG_FILE)
    @s2.start_server
  end

  after(:all) do
    @s1.kill_server
    @s2.kill_server
  end

  it 'should re-establish asymmetric route connections upon restart' do
    data = 'Hello World!'
    received = 0
    EM.run do
      timeout_em_on_failure(5)
      c1 = NATS.connect(:uri => @s1.uri)
      c2 = NATS.connect(:uri => @s2.uri)

      c1.subscribe('foo') do |msg|
        msg.should == data
        received += 1

        if received == 2
          EM.stop # proper exit
        elsif received == 1
          # Here we will kill s1, which does not actively connect to anyone.
          # Upon restart we will make sure the route was re-established properly.

          @s1.kill_server
          @s1.start_server

          wait_on_routes_connected([c1, c2]) do
            c1.connected_server.should == @s1.uri
            c2.publish('foo', data)
          end
        end
      end
      wait_on_routes_connected([c1, c2]) { c2.publish('foo', data) }
    end

    received.should == 2
  end

end
