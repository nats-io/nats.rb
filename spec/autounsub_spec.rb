require 'spec_helper'

describe 'max responses and auto-unsubscribe' do

  before(:all) do
    @s = NatsServerControl.new
    @s.start_server
  end

  after(:all) do
    @s.kill_server
  end

  it "should only receive N msgs when requested: client support" do
    WANT = 10
    SEND = 20
    received = 0
    NATS.start do
      NATS.subscribe('foo', :max => WANT) { received += 1 }
      (0...SEND).each { NATS.publish('foo', 'hello') }
      NATS.publish('done') { NATS.stop }
    end
    received.should == WANT
  end

  it "should only receive N msgs when auto-unsubscribed" do
    received = 0
    NATS.start do
      sid = NATS.subscribe('foo') { received += 1 }
      NATS.unsubscribe(sid, WANT)
      (0...SEND).each { NATS.publish('foo', 'hello') }
      NATS.publish('done') { NATS.stop }
    end
    received.should == WANT
  end

  it "should allow proper override of auto-unsubscribe max variables to lesser value" do
    received = 0
    NATS.start do
      sid = NATS.subscribe('foo') {
        received += 1
        NATS.unsubscribe(sid, 1)
      }
      NATS.unsubscribe(sid, WANT)
      (0...SEND).each { NATS.publish('foo', 'hello') }
      NATS.publish('done') { NATS.stop }
    end
    received.should == 1
  end

  it "should allow proper override of auto-unsubscribe max variables to higher value" do
    received = 0
    NATS.start do
      sid = NATS.subscribe('foo') { received += 1 }
      NATS.unsubscribe(sid, 2)
      NATS.unsubscribe(sid, WANT)
      (0...SEND).each { NATS.publish('foo', 'hello') }
      NATS.publish('done') { NATS.stop }
    end
    received.should == WANT
  end

  it "should only receive N msgs using request mode" do
    received = 0
    NATS.start do
      # Create 5 identical helpers
      (0...5).each { NATS.subscribe('help') { |msg, reply| NATS.publish(reply, 'I can help!') } }
      NATS.request('help', nil, :max => 1) { received += 1 }
      EM.add_timer(0.1) { NATS.stop }
    end
    received.should == 1
  end

end
