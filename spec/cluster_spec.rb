require 'spec_helper'
require 'yaml'

describe 'cluster' do

  before(:all) do
    SC1_CONFIG_FILE = File.dirname(__FILE__) + '/resources/s1_cluster.yml'
    @s1 = NatsServerControl.init_with_config(SC1_CONFIG_FILE)
    @s1.start_server

    SC2_CONFIG_FILE = File.dirname(__FILE__) + '/resources/s2_cluster.yml'
    @s2 = NatsServerControl.init_with_config(SC2_CONFIG_FILE)
    @s2.start_server
  end

  after(:all) do
    @s1.kill_server
    @s2.kill_server
  end

  it 'should bind a listen port for routes if configured' do
    config = File.open(SC1_CONFIG_FILE) { |f| YAML.load(f) }
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
    received = 0
    EM.run do
      c1 = NATS.connect(:uri => @s1.uri)
      c2 = NATS.connect(:uri => @s2.uri)
      c1.subscribe('foo') do |msg|
        msg.should == data
        received += 1
      end
      c2.subscribe('foo') do |msg|
        msg.should == data
        received += 1
      end
      wait_on_routes_connected([c1, c2]) do
        c2.publish('foo', data)
        c2.publish('foo', data)
        flush_routes([c1, c2]) { EM.stop }
      end
    end
    received.should == 4
  end

  it 'should properly route messages for distributed queues on different servers' do
    data = 'Hello World!'
    to_send = 100
    received = c1_received = c2_received = 0
    EM.run do
      c1 = NATS.connect(:uri => @s1.uri)
      c2 = NATS.connect(:uri => @s2.uri)
      c1.subscribe('foo', :queue => 'bar') do |msg|
        msg.should == data
        c1_received += 1
        received += 1
      end
      c2.subscribe('foo', :queue => 'bar') do |msg|
        msg.should == data
        c2_received += 1
        received += 1
      end

      wait_on_routes_connected([c1, c2]) do
        (1..to_send).each { c2.publish('foo', data) }
        flush_routes([c1, c2]) { EM.stop }
      end
    end

    received.should == to_send
    c1_received.should be < to_send
    c2_received.should be < to_send
    c1_received.should be_within(15).of(to_send/2)
    c2_received.should be_within(15).of(to_send/2)
  end

  it 'should properly route messages for distributed queues and normal subscribers on different servers' do
    data = 'Hello World!'
    to_send = 100
    received = c1_received = c2_received = 0
    EM.run do
      c1 = NATS.connect(:uri => @s1.uri)
      c2 = NATS.connect(:uri => @s2.uri)
      c1.subscribe('foo') do |msg|
        msg.should == data
        received += 1
      end
      c1.subscribe('foo', :queue => 'bar') do |msg|
        msg.should == data
        c1_received += 1
        received += 1
      end
      c2.subscribe('foo', :queue => 'bar') do |msg|
        msg.should == data
        c2_received += 1
        received += 1
      end

      wait_on_routes_connected([c1, c2]) do
        (1..to_send).each { c2.publish('foo', data) }
        flush_routes([c1, c2]) { EM.stop }
      end
    end

    received.should == to_send*2 # queue subscriber + normal subscriber
    c1_received.should be < to_send
    c2_received.should be < to_send
    c1_received.should be_within(15).of(to_send/2)
    c2_received.should be_within(15).of(to_send/2)
  end

  it 'should properly route messages for distributed queues with mulitple groups on different servers' do
    data = 'Hello World!'
    to_send = 100
    received = c1a_received = c2a_received = 0
    c1b_received = c2b_received = 0

    EM.run do
      c1 = NATS.connect(:uri => @s1.uri)
      c2 = NATS.connect(:uri => @s2.uri)

      c1.subscribe('foo') do |msg|
        msg.should == data
        received += 1
      end
      c1.subscribe('foo', :queue => 'bar') do |msg|
        msg.should == data
        c1a_received += 1
          received += 1
      end
      c1.subscribe('foo', :queue => 'baz') do |msg|
        msg.should == data
        c1b_received += 1
        received += 1
      end

      c2.subscribe('foo', :queue => 'bar') do |msg|
        msg.should == data
        c2a_received += 1
        received += 1
      end

      c2.subscribe('foo', :queue => 'baz') do |msg|
        msg.should == data
        c2b_received += 1
        received += 1
      end

      wait_on_routes_connected([c1, c2]) do
        (1..to_send).each { c2.publish('foo', data) }
        (1..to_send).each { c1.publish('foo', data) }
        flush_routes([c1, c2]) { EM.stop }
      end
    end

    received.should == to_send*6 # 2 queue subscribers + normal subscriber * 2 pub loops
    c1a_received.should be_within(15).of(to_send)
    c2a_received.should be_within(15).of(to_send)
    c1b_received.should be_within(15).of(to_send)
    c2b_received.should be_within(15).of(to_send)
  end

  it 'should properly route messages for distributed queues with reply subjects on different servers' do
    data = 'Hello World!'
    to_send = 100
    received = c1_received = c2_received = 0
    EM.run do
      c1 = NATS.connect(:uri => @s1.uri)
      c2 = NATS.connect(:uri => @s2.uri)

      c1.subscribe('foo', :queue => 'reply_test') do |msg|
        msg.should == data
        c1_received += 1
        received += 1
      end
      c2.subscribe('foo', :queue => 'reply_test') do |msg|
        msg.should == data
        c2_received += 1
        received += 1
      end
      wait_on_routes_connected([c1, c2]) do
        (1..to_send).each { c2.publish('foo', data, 'bar') }
        flush_routes([c1, c2]) { EM.stop }
      end
    end

    received.should == to_send
    c1_received.should be < to_send
    c2_received.should be < to_send
    c1_received.should be_within(25).of(to_send/2)
    c2_received.should be_within(25).of(to_send/2)
  end

end
