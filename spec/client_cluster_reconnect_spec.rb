require 'spec_helper'
require 'yaml'

describe 'client cluster reconnect' do

  before(:all) do
    S1_CONFIG_FILE = File.dirname(__FILE__) + '/resources/s1_cluster.yml'
    @s1 = NatsServerControl.init_with_config(S1_CONFIG_FILE)
    @s1.start_server

    S2_CONFIG_FILE = File.dirname(__FILE__) + '/resources/s2_cluster.yml'
    @s2 = NatsServerControl.init_with_config(S2_CONFIG_FILE)
    @s2.start_server

    S3_CONFIG_FILE = File.dirname(__FILE__) + '/resources/s3_cluster.yml'
    @s3 = NatsServerControl.init_with_config(S3_CONFIG_FILE)
    @s3.start_server
  end

  after(:all) do
    @s1.kill_server
    @s2.kill_server
    @s3.kill_server
  end

  # restart any servers that were harmed during testing..
  after(:each) do
    [@s1, @s2, @s3].each do |s|
      s.start_server unless NATS.server_running? s.uri
    end
  end

  it 'should call reconnect callback when current connection fails' do
    reconnect_cb = false
    opts = {
      :dont_randomize_servers => true,
      :reconnect_time_wait => 0.25,
      :uri => [@s1.uri, @s2.uri, @s3.uri]
    }
    NATS.start(opts) do |c|
      c.connected_server.should == @s1.uri
      timeout_nats_on_failure(0.5)
      c.on_reconnect do
        reconnect_cb = true
        NATS.stop
      end
      @s1.kill_server
    end
    reconnect_cb.should be_truthy
  end

  it 'should connect to another server if possible before reconnect' do
    NATS.start(:dont_randomize_servers => true, :servers => [@s1.uri, @s2.uri, @s3.uri]) do
      NATS.client.connected_server.should == @s1.uri
      kill_time = Time.now
      @s1.kill_server
      EM.add_timer(0.5) do
        time_diff = Time.now - kill_time
        time_diff.should < 1
        NATS.client.connected_server.should == @s2.uri
        NATS.flush { NATS.stop }
      end
    end
  end

  it 'should connect to a previous server if multiple servers exit' do
    NATS.start(:dont_randomize_servers => true, :servers => [@s1.uri, @s2.uri, @s3.uri]) do
      NATS.client.connected_server.should == @s1.uri
      kill_time = Time.now
      @s1.kill_server
      @s3.kill_server
      EM.add_timer(0.25) do
        time_diff = Time.now - kill_time
        time_diff.should < 1
        NATS.client.connected_server.should == @s2.uri
        @s1.start_server
        kill_time2 = Time.now
        @s2.kill_server
        EM.add_timer(0.25) do
          time_diff = Time.now - kill_time2
          time_diff.should < 1
          NATS.client.connected_server.should == @s1.uri
          NATS.stop
        end
      end
    end
  end

  it 'should use reconnect logic to connect to a previous server if multiple servers exit' do
    @s2.kill_server # Take this one offline
    options = {
      :dont_randomize_servers => true,
      :servers => [@s1.uri, @s2.uri],
      :reconnect => true,
      :max_reconnect_attempts => 1,
      :reconnect_time_wait => 0.5
    }

    NATS.start(options) do
      NATS.client.connected_server.should == @s1.uri
      kill_time = Time.now
      @s1.kill_server
      EM.add_timer(0.25) { @s1.start_server }
      EM.add_timer(1.5) do
        time_diff = Time.now - kill_time
        time_diff.should < 2
        NATS.client.connected_server.should == @s1.uri
        NATS.stop
      end
    end
  end

end
