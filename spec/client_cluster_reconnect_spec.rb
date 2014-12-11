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

  it 'should properly handle exceptions thrown by eventmachine during reconnects' do
    reconnect_cb = false
    opts = {
      :dont_randomize_servers => true,
      :reconnect_time_wait => 0.25,
      :uri => [@s1.uri, URI.parse("nats://does.not.exist:4222/"), @s3.uri]
    }
    NATS.start(opts) do |c|
      timer=timeout_nats_on_failure(1)
      c.on_reconnect do
        reconnect_cb = true
        NATS.connected?.should be_truthy
        NATS.connected_server.should == @s3.uri
        NATS.stop
      end
      @s1.kill_server
    end
    reconnect_cb.should be_truthy
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
    NATS.start(:dont_randomize_servers => true, :servers => [@s1.uri, @s2.uri, @s3.uri]) do |c|
      timeout_nats_on_failure(15)
      c.connected_server.should == @s1.uri
      c.on_reconnect do
        c.connected_server.should == @s2.uri
        NATS.stop
      end
      @s1.kill_server
    end
  end

  it 'should connect to a previous server if multiple servers exit' do
    NATS.start(:dont_randomize_servers => true, :servers => [@s1.uri, @s2.uri, @s3.uri]) do |c|
      timeout_nats_on_failure(15)
      c.connected_server.should == @s1.uri
      kill_time = Time.now

      expected_uri = nil
      c.on_reconnect do
        c.connected_server.should == expected_uri
        if expected_uri == @s2.uri
          # Expect to connect back to S1
          expected_uri = @s1.uri
          @s1.start_server
          @s2.kill_server
        end
        NATS.stop if c.connected_server == @s1.uri
      end

      # Expect to connect to S2 after killing S1 and S3.
      expected_uri = @s2.uri

      @s1.kill_server
      @s3.kill_server
    end
  end

  it 'should use reconnect logic to connect to a previous server if multiple servers exit' do
    @s2.kill_server # Take this one offline
    options = {
      :dont_randomize_servers => true,
      :servers => [@s1.uri, @s2.uri],
      :reconnect => true,
      :max_reconnect_attempts => 2,
      :reconnect_time_wait => 1
    }
    NATS.start(options) do |c|
      timeout_nats_on_failure(15)
      c.connected_server.should == @s1.uri
      c.on_reconnect do
        c.connected?.should be_truthy
        c.connected_server.should == @s1.uri
        NATS.stop
      end
      @s1.kill_server
      @s1.start_server
    end
  end

end
