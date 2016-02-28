require 'spec_helper'

describe 'subscriptions with timeouts' do

  before(:all) do
    TIMEOUT  = 0.1
    WAIT     = 0.2
    @s = NatsServerControl.new
    @s.start_server
  end

  after(:all) do
    @s.kill_server
  end

  it "a subscription should not receive a message after a timeout" do
    received = 0
    NATS.start do
      sid = NATS.subscribe('foo') { received += 1 }
      NATS.timeout(sid, TIMEOUT)
      EM.add_timer(WAIT) { NATS.publish('foo') { NATS.stop } }
    end
    received.should == 0
  end

  it "a subsciption should call the timeout callback if no messages are received" do
    received = 0
    timeout_recvd = false
    NATS.start do
      sid = NATS.subscribe('foo') { received += 1 }
      NATS.timeout(sid, TIMEOUT) { timeout_recvd = true }
      EM.add_timer(WAIT) { NATS.stop }
    end
    timeout_recvd.should be_truthy
    received.should == 0
  end

  it "a subsciption should call the timeout callback if no messages are received, connection version" do
    received = 0
    timeout_recvd = false
    NATS.start do |c|
      sid = c.subscribe('foo') { received += 1 }
      c.timeout(sid, TIMEOUT) { timeout_recvd = true }
      EM.add_timer(WAIT) { NATS.stop }
    end
    timeout_recvd.should be_truthy
    received.should == 0
  end

  it "a subsciption should not call the timeout callback if a message is received" do
    received = 0
    timeout_recvd = false
    NATS.start do
      sid = NATS.subscribe('foo') { received += 1 }
      NATS.timeout(sid, TIMEOUT) { timeout_recvd = true }
      NATS.publish('foo')
      NATS.publish('foo')
      EM.add_timer(WAIT) { NATS.stop }
    end
    timeout_recvd.should be_falsey
    received.should == 2
  end

  it "a subsciption should not call the timeout callback if a correct # messages are received" do
    received = 0
    timeout_recvd = false
    NATS.start do
      sid = NATS.subscribe('foo') { received += 1 }
      NATS.timeout(sid, TIMEOUT, :expected => 2) { timeout_recvd = true }
      NATS.publish('foo')
      NATS.publish('foo')
      EM.add_timer(WAIT) { NATS.stop }
    end
    timeout_recvd.should be_falsey
    received.should == 2
  end

  it "a subsciption should call the timeout callback if a correct # messages are not received" do
    received = 0
    timeout_recvd = false
    NATS.start do
      sid = NATS.subscribe('foo') { received += 1 }
      NATS.timeout(sid, TIMEOUT, :expected => 2) { timeout_recvd = true }
      NATS.publish('foo')
      EM.add_timer(WAIT) { NATS.publish('foo') { NATS.stop} }
    end
    timeout_recvd.should be_truthy
    received.should == 1
  end

  it "a subsciption should call the timeout callback and message callback if requested" do
    received = 0
    timeout_recvd = false
    NATS.start do
      sid = NATS.subscribe('foo') { received += 1 }
      NATS.timeout(sid, TIMEOUT, :auto_unsubscribe => false) { timeout_recvd = true }
      EM.add_timer(WAIT) { NATS.publish('foo') { NATS.stop} }
    end
    timeout_recvd.should be_truthy
    received.should == 1
  end

end
