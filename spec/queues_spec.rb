require 'spec_helper'

describe "queue group support" do

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
      s1 = NATS.subscribe('foo', :queue => 'g1') { received += 1 }
      s1.should_not be_nil
      s2 = NATS.subscribe('foo', :queue => 'g1') { received += 1 }
      s2.should_not be_nil
      NATS.publish('foo', 'hello') { NATS.stop }
    end
    received.should == 1
  end

  it "should allow queue receivers and normal receivers to work together" do
    received = 0
    NATS.start do
      (0...5).each { NATS.subscribe('foo', :queue => 'g1') { received += 1 } }
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
        NATS.subscribe('foo.bar', :queue => 'queue_group_1') do
          received[i] = received[i] + 1
          total += 1
        end
      end
      (0...TOTAL).each { NATS.publish('foo.bar', 'ok') }
      NATS.flush { NATS.stop }
    end
    received.each_value { |count| (AVG - count).abs.should < ALLOWED_V }
    total.should == TOTAL
  end

  it "should deliver a message to only one subscriber in a queue group, regardless of wildcard subjects" do
    received = 0
    NATS.start do
      NATS.subscribe('foo.bar', :queue => 'g1') { received += 1 }
      NATS.subscribe('foo.*', :queue => 'g1') { received += 1 }
      NATS.subscribe('foo.>', :queue => 'g1') { received += 1 }
      NATS.publish('foo.bar', 'hello') { NATS.stop }
    end
    received.should == 1
  end

  it "should deliver 1 message/group for each publish" do
    received_g1 = 0
    received_g2 = 0
    NATS.start do
      NATS.subscribe('foo.bar', :queue => 'g1') { received_g1 += 1 }
      5.times do
        NATS.subscribe('foo.bar', :queue => 'g2') { received_g2 += 1 }
      end

      10.times do
        NATS.publish('foo.bar', 'hello')
      end
      NATS.flush { NATS.stop }
    end
    received_g1.should == 10
    received_g2.should == 10
  end

  it "should re-establish queue groups on reconnect" do
    buckets = Hash.new(0)

    num_to_send = 1000

    reconnect_sid = false
    reconnect_timer = nil

    NATS.start(:reconnect_time_wait => 0.25) do |conn|

      reconnect_sid = NATS.subscribe("reconnected_trigger") do
        NATS.publish("control", "reconnected")
        NATS.unsubscribe(reconnect_sid)
        EM.cancel_timer(reconnect_timer)
      end

      2.times do |ii|
        NATS.subscribe("test_queue", :queue => "test_queue") do
          buckets[ii] += 1
          NATS.publish("control", "ack")
        end
      end

      # Don't hang indefinitely
      EM.add_timer(30) { EM.stop }

      total_acked = 0

      # Ensure the queue subscribes have been processed
      NATS.flush do
        NATS.subscribe("control") do |state|
          case state
          when "start"
            @s.kill_server
            @s.start_server
            reconnect_timer = EM.add_periodic_timer(0.25) do
              NATS.publish("reconnected_trigger")
            end
          when "reconnected"
            num_to_send.times { NATS.publish("test_queue") }
          when "ack"
            total_acked +=1
            NATS.stop if total_acked == num_to_send
          else
            puts "Unexpected message: #{state}"
            NATS.stop
          end
        end
      end

      NATS.flush { NATS.publish("control", "start") }
    end

    # Messages are distributed uniformly at random to queue subscribers. This
    # forms a binomal distribution with a probability of success (receiving a
    # message) of .5 in the two subscriber case. This verifies that the receive
    # count of each subscriber is within 3 standard deviations of the mean.
    # In theory, this means that the test will fail ~ .3% of the time.
    buckets.values.each { |msg_count| msg_count.should be_within(150).of(500) }
  end
end
