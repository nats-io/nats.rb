require 'spec_helper'
require 'yaml'

describe 'cluster' do

  # FIXME, delete log files

  before(:all) do
    S1_CONFIG_FILE = File.dirname(__FILE__) + '/resources/s1_cluster.yml'
    @s1 = NatsServerControl.init_with_config(S1_CONFIG_FILE)
    @s1.start_server

    S2_CONFIG_FILE = File.dirname(__FILE__) + '/resources/s2_cluster.yml'
    @s2 = NatsServerControl.init_with_config(S2_CONFIG_FILE)
    @s2.start_server
  end

  after(:all) do
    @s1.kill_server
    @s2.kill_server
  end

  it 'should bind a listen port for routes if configured' do
    config = File.open(S1_CONFIG_FILE) { |f| YAML.load(f) }
    expect do
      begin
        s = TCPSocket.open(config['net'], config['cluster']['port'])
      ensure
        s.close if s
      end
    end.to_not raise_error
  end

  it 'should properly connect to different servers' do
    EM.run do
      c1 = NATS.connect(:uri => @s1.uri)
      c2 = NATS.connect(:uri => @s2.uri)
      wait_on_connections([c1, c2]) do
        EM.stop
      end
    end
  end

  it 'should properly route plain messages between different servers' do
    data = 'Hello World!'
    expect, received = 2, 0
    EM.run do
      c1 = NATS.connect(:uri => @s1.uri)
      c2 = NATS.connect(:uri => @s2.uri)
      wait_on_connections([c1, c2]) do
        c1.subscribe('foo') do |msg|
          msg.should == data
          received += 1
        end
        c1.flush do #make sure sub registered
          c2.publish('foo', data)
          c2.publish('foo', data)
          c2.flush do #make sure published
            c1.flush do #make sure received
              EM.stop
            end
          end
        end
      end
    end
    received.should == 2
  end

  it 'should properly route messages for distributed queues with consumers on different servers'

end
