require 'spec_helper'
require 'monitor'

describe 'Client - Reconnect' do

  before(:each) do
    @s = NatsServerControl.new
    @s.start_server(true)
  end

  after(:each) do
    @s.kill_server
  end

  it 'should reconnect to server and replay all subscriptions' do
    msgs = []
    errors = []
    closes = 0
    reconnects = 0
    disconnects = 0

    nats = NATS::IO::Client.new
    mon = Monitor.new
    done = mon.new_cond

    nats.on_error do |e|
      errors << e
    end

    nats.on_reconnect do
      reconnects += 1
    end

    nats.on_disconnect do
      disconnects += 1
    end

    nats.on_close do
      closes += 1
      mon.synchronize do
        done.signal
      end
    end

    nats.connect

    nats.subscribe("foo") do |msg|
      msgs << msg
    end

    nats.subscribe("bar") do |msg|
      msgs << msg
    end
    nats.flush

    nats.publish("foo", "hello.0")
    @s.kill_server

    1.upto(10).each do |n|
      nats.publish("foo", "hello.#{n}")
      sleep 0.1
    end
    @s.start_server(true)
    sleep 1

    mon.synchronize { done.wait(1) }
    expect(disconnects).to eql(1)
    expect(msgs.count).to eql(10)

    # Cannot guarantee to get all of them since the server
    # was interrupted during send but at least some which
    # were pending during reconnect should have made it.
    expect(msgs.count > 5).to eql(true)
    expect(nats.status).to eql(NATS::IO::CONNECTED)
  end

  it 'should abort reconnecting if disabled' do
    msgs = []
    errors = []
    closes = 0
    reconnects = 0
    disconnects = 0

    nats = NATS::IO::Client.new
    mon = Monitor.new
    done = mon.new_cond

    nats.on_error do |e|
      errors << e
    end

    nats.on_reconnect do
      reconnects += 1
    end

    nats.on_disconnect do
      disconnects += 1
    end

    nats.on_close do
      closes += 1
      mon.synchronize { done.signal }
    end

    nats.connect(:reconnect => false)

    nats.subscribe("foo") do |msg|
      msgs << msg
    end

    nats.subscribe("bar") do |msg|
      msgs << msg
    end
    nats.flush

    nats.publish("foo", "hello")
    @s.kill_server

    10.times do
      nats.publish("foo", "hello")
      sleep 0.01
    end

    # Wait for a bit before checking state again
    mon.synchronize { done.wait(1) }
    expect(nats.last_error).to be_a(Errno::ECONNRESET)
    expect(nats.status).to eql(NATS::IO::CLOSED)
  end
end
