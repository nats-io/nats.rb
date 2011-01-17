require 'spec_helper'

describe NATS do

  before(:all) do
    @s = NatsServerControl.new
    @s.start_server
  end

  after(:all) do
    @s.kill_server
  end

  it "should deliver a message to only one subscriber in a queue group" do
    received = 0
    NATS.start do
      s1 = NATS.subscribe('foo', 'g1') { received += 1 }
      s1.should_not be_nil
      s2 = NATS.subscribe('foo', 'g1') { received += 1 }
      s2.should_not be_nil
      NATS.publish('foo', 'hello') { NATS.stop }
    end
    received.should == 1
  end

  it "should allow queue receivers and normal receivers to work together" do
    received = 0
    NATS.start do
      (0...5).each { NATS.subscribe('foo', 'g1') { received += 1 } }
      NATS.subscribe('foo') { received += 1 }
      NATS.publish('foo', 'hello') { NATS.stop }
    end
    received.should == 2
  end

  it "should spread messages equally across multiple receivers" do
    TOTAL = 5000
    NUM_SUBSCRIBERS = 10
    AVG = TOTAL / NUM_SUBSCRIBERS
    ALLOWED_V = TOTAL * 0.05
    received = Hash.new(0)
    total = 0
    NATS.start do
      (0...NUM_SUBSCRIBERS).each do |i|
        NATS.subscribe('foo.bar', 'queue_group_1') do
          received[i] = received[i] + 1
          total += 1
        end
      end
      (0...TOTAL).each { NATS.publish('foo.bar', 'ok') }
      NATS.publish('done') { NATS.stop }
    end
    received.each_value { |count| (AVG - count).abs.should < ALLOWED_V }
    total.should == TOTAL
  end

  it "should deliver a message to only one subscriber in a queue group, regardless of wildcard subjects" do
    received = 0
    NATS.start do
      NATS.subscribe('foo.bar', 'g1') { received += 1 }
      NATS.subscribe('foo.*', 'g1') { received += 1 }
      NATS.subscribe('foo.>', 'g1') { received += 1 }
      NATS.publish('foo.bar', 'hello') { NATS.stop }
    end
    received.should == 1
  end

end
