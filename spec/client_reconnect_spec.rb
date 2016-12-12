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
    nats.flush
    @s.kill_server

    1.upto(10).each do |n|
      nats.publish("foo", "hello.#{n}")
      sleep 0.1
    end
    @s.start_server(true)
    sleep 1

    mon.synchronize { done.wait(1) }
    expect(disconnects).to eql(1)
    expect(msgs.count).to eql(11)
    expect(reconnects).to eql(1)
    expect(closes).to eql(0)

    # Cannot guarantee to get all of them since the server
    # was interrupted during send but at least some which
    # were pending during reconnect should have made it.
    expect(msgs.count > 5).to eql(true)
    expect(nats.status).to eql(NATS::IO::CONNECTED)

    nats.close
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

    nats.close
  end

  it 'should give up connecting if no servers available' do
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

    expect do
      nats.connect({
       :servers => ["nats://127.0.0.1:4229"],
       :max_reconnect_attempts => 2,
       :reconnect_time_wait => 1
      })
    end.to raise_error(NATS::IO::NoServersError)

    # Confirm that we have captured the sticky error
    # and that the connection has remained disconnected.
    expect(errors.first).to be_a(Errno::ECONNREFUSED)
    expect(errors.count).to eql(3)
    expect(nats.last_error).to be_a(Errno::ECONNREFUSED)
    expect(nats.status).to eql(NATS::IO::DISCONNECTED)
  end

  it 'should give up reconnecting if no servers available' do
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

    nats.connect({
     :servers => ["nats://127.0.0.1:4222"],
     :max_reconnect_attempts => 1,
     :reconnect_time_wait => 1
    })

    nats.subscribe("foo") do |msg|
      msgs << msg
    end

    nats.subscribe("bar") do |msg|
      msgs << msg
    end
    nats.flush

    nats.publish("foo", "hello.0")
    nats.flush
    @s.kill_server

    1.upto(10).each do |n|
      nats.publish("foo", "hello.#{n}")
      sleep 0.1
    end

    # Confirm that we have captured the sticky error
    # and that the connection is closed due no servers left.
    mon.synchronize { done.wait(5) }
    expect(disconnects).to eql(1)
    expect(reconnects).to eql(0)
    expect(closes).to eql(0)
    expect(nats.last_error).to be_a(NATS::IO::NoServersError)
    expect(errors.first).to be_a(Errno::ECONNREFUSED)
    expect(errors.count).to eql(2)
    expect(nats.status).to eql(NATS::IO::CLOSED)
  end
end
