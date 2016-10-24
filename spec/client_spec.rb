require 'spec_helper'
require 'monitor'

describe 'Client - Specification' do

  before(:each) do
    @s = NatsServerControl.new
    @s.start_server(true)
  end

  after(:each) do
    @s.kill_server
    sleep 1
  end

  it 'should connect to locally available server by default' do
    nc = NATS::IO::Client.new
    expect do
      nc.connect(:servers => ["nats://127.0.0.1:4222"])
    end.to_not raise_error
    nc.close
  end

  it 'should received a message when subscribed to a topic' do
    nc = NATS::IO::Client.new
    nc.connect(:servers => [@s.uri])

    msgs = []
    nc.subscribe("hello") do |msg|
      msgs << msg
    end
    sleep 0.5

    1.upto(5) do |n|
      nc.publish("hello", "world-#{n}")
    end
    sleep 0.5

    expect(msgs.count).to eql(5)
    expect(msgs.first).to eql('world-1')
    expect(msgs.last).to eql('world-5')

    nc.close
  end

  it 'should be able to receive requests synchronously with a timeout' do
    nc = NATS::IO::Client.new
    nc.connect(:servers => ["nats://127.0.0.1:4222"])

    received = []
    nc.subscribe("help") do |msg, reply, subject|
      received << msg
      nc.publish(reply, "reply.#{received.count}")
    end
    nc.flush

    responses = []
    responses << nc.timed_request("help", 'please', 1)
    responses << nc.timed_request("help", 'again', 1)
    expect(responses.count).to eql(2)
    expect(responses.first[:data]).to eql('reply.1')
    expect(responses.last[:data]).to eql('reply.2')

    nc.close
  end

  it 'should be able to receive limited requests asynchronously' do
    mon = Monitor.new
    done = mon.new_cond

    nc = NATS::IO::Client.new
    nc.connect(:servers => ["nats://127.0.0.1:4222"])

    received = []
    nc.subscribe("help") do |msg, reply, subject|
      received << msg
      nc.publish(reply, "reply")
      nc.publish(reply, "back")
      nc.publish(reply, "wont be received")
      nc.publish(reply, "wont be received either")
    end
    nc.flush

    responses = []
    nc.request("help", "please", :max => 2) do |msg|
      responses << msg
      mon.synchronize do
        done.signal
      end
    end

    mon.synchronize do
      done.wait(1)
    end
    nc.close

    expect(received.count).to eql(1)
    expect(responses.count).to eql(2)
    expect(responses[0]).to eql("reply")
    expect(responses[1]).to eql("back")
  end

  it 'should be able to unsubscribe' do
    nc = NATS::IO::Client.new
    nc.connect(:servers => ["nats://127.0.0.1:4222"], :reconnect => false)

    msgs = []
    sid = nc.subscribe("foo") do |msg|
      msgs << msg
    end
    expect(sid).to eql(1)
    nc.flush

    2.times { nc.publish("foo", "bar") }
    nc.flush

    nc.unsubscribe(sid)
    nc.flush

    2.times { nc.publish("foo", "bar") }
    nc.flush

    expect(msgs.count).to eql(2)
    nc.close
  end

  it 'should be able to create many subscriptions' do
    nc = NATS::IO::Client.new
    nc.connect(:servers => ["nats://127.0.0.1:4222"])

    msgs = { }
    1.upto(100).each do |n|
      sid = nc.subscribe("quux.#{n}") do |msg, reply, subject|
        msgs[subject] << msg
      end
      msgs["quux.#{sid}"] = []
    end
    nc.flush

    expect(msgs.keys.count).to eql(100)
    1.upto(100).each do |n|
      nc.publish("quux.#{n}")
    end
    nc.flush

    1.upto(100).each do |n|
      expect(msgs["quux.#{n}"].count).to eql(1)
    end

    nc.close
  end

  it 'should be raise timeout error if timed request does not get response' do
    nc = NATS::IO::Client.new
    nc.connect(:servers => ["nats://127.0.0.1:4222"])

    expect do
      nc.timed_request("hello", "timeout")
    end.to raise_error(NATS::IO::Timeout)

    nc.close
  end

  it 'should be able to receive response to requests' do
    mon = Monitor.new
    subscribed_done = mon.new_cond
    test_done = mon.new_cond

    another_thread = Thread.new do
      nats = NATS::IO::Client.new
      nats.connect(:servers => ["nats://127.0.0.1:4222"], :reconnect => false)
      nats.subscribe("help") do |msg, reply|
        nats.publish(reply, "I can help")
      end
      nats.flush
      mon.synchronize do
        subscribed_done.signal
      end
      mon.synchronize do
        test_done.wait(1)
        nats.close
      end
    end

    nc = NATS::IO::Client.new
    nc.connect(:servers => ["nats://127.0.0.1:4222"])
    mon.synchronize do
      subscribed_done.wait(1)
    end

    responses = []
    expect do
      3.times do
        responses << nc.timed_request("help", "please", 1)
      end
    end.to_not raise_error
    expect(responses.count).to eql(3)

    # A new subscription would have the next sid for this client
    sid = nc.subscribe("hello"){}
    expect(sid).to eql(4)
    mon.synchronize do
      test_done.signal
    end

    nc.close
  end

  it 'should close connection gracefully' do
    mon = Monitor.new
    test_is_done = mon.new_cond

    nats = NATS::IO::Client.new
    nats.connect(:servers => ["nats://127.0.0.1:4222"], :reconnect => false)

    errors = []
    disconnects = 0
    closes = 0

    nats.on_error do |e|
      errors << e
    end

    nats.on_disconnect do
      disconnects += 1
    end

    nats.on_close do
      closes += 1
      mon.synchronize do
        test_is_done.signal
      end
    end

    msgs = []
    nats.subscribe("bar.>") do |msg|
      msgs << msg
    end
    nats.flush

    pub_thread = Thread.new do
      1.upto(10000).each do |n|
        nats.publish("bar.#{n}", "A" * 10)
      end
      sleep 0.01
      10001.upto(20000).each do |n|
        nats.publish("bar.#{n}", "B" * 10)
      end
    end
    pub_thread.abort_on_exception = true

    nats.close
    mon.synchronize do
      test_is_done.wait(1)
    end

    expect(nats.status).to eql(NATS::IO::CLOSED)
    expect(errors).to be_empty
    expect(disconnects).to eql(1)
    expect(closes).to eql(1)

    # Make sure to kill the publishing thread
    pub_thread.kill
  end
end
