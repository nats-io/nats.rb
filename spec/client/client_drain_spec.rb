require 'spec_helper'

describe 'Client - Drain' do

  before(:each) do
    @s = NatsServerControl.new
    @s.start_server(true)
  end

  after(:each) do
    @s.kill_server
  end

  it "should support draining a connection" do
    msgs = []
    errors = []
    closed = false
    drained = false
    after_drain = nil
    total_msgs_before_drain = nil
    total_msgs_after_drain = nil
    pending_data_before_draining = nil
    pending_data_after_draining = nil

    with_em_timeout(10) do |future|
      nc1 = NATS.connect(uri: @s.uri) do |nc|
        expect(nc.options[:drain_timeout]).to eql(30)
        nc.on_error do |err|
          errors << err
        end
        nc.on_close do |err|
          closed = true
          future.resume
        end

        nc.subscribe("foo", queue: "worker") do |msg, reply|
          nc.publish(reply, "ACK:foo")
        end

        nc.subscribe("bar") do |msg, reply|
          nc.publish(reply, "ACK:bar")
        end

        nc.subscribe("quux") do |msg, reply|
          nc.publish(reply, "ACK:quux")
        end

        EM.add_timer(1) do
          # Before draining
          subs = nc.instance_variable_get('@subs')
          pending_data_before_draining = nc.instance_variable_get('@buf')
          total_msgs_before_drain = subs.reduce(0) do |total, pair|
            sid, sub = pair
            total += sub[:received]
          end

          nc.drain do
            after_drain = nc.draining?
            drained = true

            pending_data_after_draining = nc.instance_variable_get('@buf')
            subs = nc.instance_variable_get('@subs')
            total_msgs_after_drain = subs.reduce(0) do |total, pair|
              sid, sub = pair
              total += sub[:received]
            end
          end
        end
      end

      # Fast publisher
      nc2 = NATS.connect(uri: @s.uri) do |nc|
        inbox = NATS.create_inbox
        nc.subscribe(inbox) do |msg|
          msgs << msg
        end

        timer = EM.add_periodic_timer(0.1) do
          10000.times do
            nc.publish("foo", "hi", inbox)
            nc.publish("bar", "hi", inbox)
            nc.publish("quux", "hi", inbox)
          end
        end
        EM.add_timer(1) do
          EM.cancel_timer(timer)
        end
      end
    end
    expect(errors.count).to eql(0)
    expect(closed).to eql(true)
    expect(drained).to eql(true)
    expect(after_drain).to eql(false)
    expect(total_msgs_before_drain < total_msgs_after_drain).to eql(true)
    expect(pending_data_after_draining).to eql(nil)
  end

  it "should timeout draining if takes too long" do
    msgs = []
    errors = []
    closed = false
    drained = false
    after_drain = nil
    total_msgs_before_drain = nil
    total_msgs_after_drain = nil
    pending_data_before_draining = nil
    pending_data_after_draining = nil

    with_em_timeout(10) do |future|
      # Use a very short timeout for to timeout.
      nc1 = NATS.connect(uri: @s.uri, drain_timeout: 0.01) do |nc|
        nc.on_error do |err|
          errors << err
        end
        nc.on_close do |err|
          closed = true
          future.resume
        end

        nc.subscribe("foo", queue: "worker") do |msg, reply|
          nc.publish(reply, "ACK:foo")
        end

        nc.subscribe("bar") do |msg, reply|
          nc.publish(reply, "ACK:bar")
        end

        nc.subscribe("quux") do |msg, reply|
          nc.publish(reply, "ACK:quux")
        end

        EM.add_timer(1) do
          # Before draining
          subs = nc.instance_variable_get('@subs')
          pending_data_before_draining = nc.instance_variable_get('@buf')
          total_msgs_before_drain = subs.reduce(0) do |total, pair|
            sid, sub = pair
            total += sub[:received]
          end

          nc.drain do
            after_drain = nc.draining?
            drained = true

            pending_data_after_draining = nc.instance_variable_get('@buf')
            subs = nc.instance_variable_get('@subs')
            total_msgs_after_drain = subs.reduce(0) do |total, pair|
              sid, sub = pair
              total += sub[:received]
            end
          end
        end
      end

      # Fast publisher
      nc2 = NATS.connect(uri: @s.uri) do |nc|
        inbox = NATS.create_inbox
        nc.subscribe(inbox) do |msg|
          msgs << msg
        end

        timer = EM.add_periodic_timer(0.1) do
          10000.times do
            nc.publish("foo", "hi", inbox)
            nc.publish("bar", "hi", inbox)
            nc.publish("quux", "hi", inbox)
          end
        end
        EM.add_timer(1) do
          EM.cancel_timer(timer)
        end
      end
    end
    expect(errors.count).to eql(1)
    expect(errors.first).to be_a(NATS::ClientError)
    expect(errors.first.to_s).to eql("Drain Timeout")
    expect(closed).to eql(true)
    expect(drained).to eql(true)
    expect(after_drain).to eql(false)
    expect(total_msgs_before_drain < total_msgs_after_drain).to eql(true)
    expect(pending_data_after_draining).to eql(nil)
  end
end
