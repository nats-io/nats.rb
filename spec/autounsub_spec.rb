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

  it "should not complain when unsubscribing an auto-unsubscribed sid" do
    received = 0
    NATS.start do
      sid = NATS.subscribe('foo', :max => 1) { received += 1 }
      (0...SEND).each { NATS.publish('foo', 'hello') }
      NATS.publish('done') {
        NATS.unsubscribe(sid)
        NATS.stop
      }
    end
    received.should == 1
  end

  it "should allow proper override of auto-unsubscribe max variables to lesser value" do
    received = 0
    NATS.start do
      sid = NATS.subscribe('foo') {
        received += 1
        NATS.unsubscribe(sid, 1)
      }
      NATS.unsubscribe(sid, SEND+1)
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

  it "should only receive N msgs using request mode with multiple helpers" do
    received = 0
    NATS.start do
      # Create 5 identical helpers
      (0...5).each { NATS.subscribe('help') { |msg, reply| NATS.publish(reply, 'I can help!') } }
      NATS.request('help', nil, :max => 1) { received += 1 }
      EM.add_timer(0.1) { NATS.stop }
    end
    received.should == 1
  end

  it "should not leak subscriptions on request that auto-unsubscribe properly with :max" do
    received = 0
    NATS.start do
      sid = NATS.subscribe('help') { |msg, reply| NATS.publish(reply, 'I can help!') }
      (1..100).each do
        NATS.request('help', 'help request', :max => 1) { received += 1 }
      end
      NATS.flush do
        EM.add_timer(0.1) do
          NATS.unsubscribe(sid)
          NATS.client.subscription_count.should == 0
          NATS.stop
        end
      end
    end
    received.should == 100
  end

  it "should not complain when unsunscribe called on auto-cleaned up subscription" do
    NATS.start do
      sid = NATS.subscribe('help') { |msg, reply| NATS.publish(reply, 'I can help!') }
      rsid = NATS.request('help', 'help request', :max => 1) {}
      NATS.flush do
        EM.add_timer(0.1) do
          NATS.client.subscription_count.should == 1
          NATS.unsubscribe(sid)
          NATS.client.subscription_count.should == 0
          NATS.unsubscribe(rsid)
          NATS.stop
        end
      end
    end
  end

end
