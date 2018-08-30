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
    total_msgs_sent = nil
    pending_data_before_draining = nil
    pending_data_after_draining = nil
    pending_outbound_data_before_draining = nil
    pending_outbound_data_after_draining = nil

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
          pending_data_before_draining = nc.instance_variable_get('@buf')
          pending_outbound_data_before_draining = nc.pending_data_size

          total_msgs_before_drain = nc.msgs_received
          nc.drain do
            after_drain = nc.draining?
            drained = true

            total_msgs_sent = nc.msgs_sent
            total_msgs_after_drain = nc.msgs_received
            pending_data_after_draining = nc.instance_variable_get('@buf')
            pending_outbound_data_after_draining = nc.pending_data_size
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

    # Should be the same as the messages received by the first client.
    expect(msgs.count).to eql(total_msgs_sent)
    expect(msgs.count).to eql(total_msgs_after_drain)
    expect(pending_outbound_data_after_draining).to eql(0)
    expect(pending_data_after_draining).to eql(nil)
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
    total_msgs_sent = nil
    pending_data_before_draining = nil
    pending_data_after_draining = nil
    pending_outbound_data_before_draining = nil
    pending_outbound_data_after_draining = nil

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
          subs = nc.instance_variable_get('@subs')
          pending_data_before_draining = nc.instance_variable_get('@buf')
          total_msgs_before_drain = nc.msgs_received

          nc.drain do
            after_drain = nc.draining?
            drained = true

            pending_data_after_draining = nc.instance_variable_get('@buf')
            total_msgs_after_drain = nc.msgs_received
            total_msgs_sent = nc.msgs_sent
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

  it "should disallow subscribe and unsubscribe while draining" do
    msgs = []
    errors = []
    closed = false
    drained = false
    after_drain = nil
    total_msgs_before_drain = nil
    total_msgs_after_drain = nil
    total_msgs_sent = nil
    pending_data_before_draining = nil
    pending_data_after_draining = nil
    pending_outbound_data_before_draining = nil
    pending_outbound_data_after_draining = nil
    unsub_result = true

    no_more_subs = nil
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

        sub_timer = EM.add_periodic_timer(0.1) do
          sid = nc.subscribe("hello.#{rand(1_000_000)}") { }

          if sid.nil?
            no_more_subs = true 
            EM.cancel_timer(sub_timer)

            # Any sid even if invalid should return right away
            unsub_result = nc.unsubscribe(1)
          end
        end

        EM.add_timer(1) do
          pending_data_before_draining = nc.instance_variable_get('@buf')
          pending_outbound_data_before_draining = nc.pending_data_size

          total_msgs_before_drain = nc.msgs_received
          nc.drain do
            after_drain = nc.draining?
            drained = true

            total_msgs_sent = nc.msgs_sent
            total_msgs_after_drain = nc.msgs_received
            pending_data_after_draining = nc.instance_variable_get('@buf')
            pending_outbound_data_after_draining = nc.pending_data_size
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

    # Subscribe should have eventually failed
    expect(no_more_subs).to eql(true)
    expect(unsub_result).to eql(nil)

    # Should be the same as the messages received by the first client.
    expect(msgs.count).to eql(total_msgs_sent)
    expect(msgs.count).to eql(total_msgs_after_drain)
    expect(pending_outbound_data_after_draining).to eql(0)
    expect(pending_data_after_draining).to eql(nil)
    expect(errors.count).to eql(0)
    expect(closed).to eql(true)
    expect(drained).to eql(true)
    expect(after_drain).to eql(false)
    expect(total_msgs_before_drain < total_msgs_after_drain).to eql(true)
    expect(pending_data_after_draining).to eql(nil)
  end

  it "should disallow publish and flush outbound pending data once subscriptions have been drained" do
    msgs = []
    errors = []
    closed = false
    drained = false
    after_drain = nil

    total_msgs_received_before_drain = nil
    total_msgs_received_after_drain = nil
    total_msgs_sent = nil

    pending_data_before_draining = nil
    pending_data_after_draining = nil
    pending_outbound_data_before_draining = nil
    pending_outbound_data_after_draining = nil

    before_publish = nil
    after_publish = nil
    extra_pubs = 0
    with_em_timeout(30) do |future|
      nc1 = NATS.connect(uri: @s.uri) do |nc|
        expect(nc.options[:drain_timeout]).to eql(30)
        nc.on_error do |err|
          errors << err
        end
        nc.on_close do |err|
          closed = true

          # Give sometime to the other client to receive
          # all the messages that were published by client
          # that started to drain.
          EM.add_timer(5) do
            future.resume
          end
        end

        nc.subscribe("foo", queue: "worker") do |msg, reply|
          10.times { nc.publish(reply, "ACK:foo") }
        end

        nc.subscribe("bar") do |msg, reply|
          10.times { nc.publish(reply, "ACK:bar") }
        end

        nc.subscribe("quux") do |msg, reply|
          10.times { nc.publish(reply, "ACK:quux") }
        end

        EM.add_timer(0.5) do
          pub_timer = EM.add_periodic_timer(0.1) do
            before_publish = nc.msgs_sent
            nc.publish("hello", "world")
            after_publish = nc.msgs_sent
            if before_publish == after_publish
              EM.cancel_timer(pub_timer)
            else
              extra_pubs += 1
            end
          end
        end

        EM.add_timer(1.5) do
          pending_data_before_draining = nc.instance_variable_get('@buf')
          pending_outbound_data_before_draining = nc.pending_data_size

          total_msgs_received_before_drain = nc.msgs_received
          nc.drain do
            after_drain = nc.draining?
            drained = true
            total_msgs_sent = nc.msgs_sent
            total_msgs_received_after_drain = nc.msgs_received
            pending_data_after_draining = nc.instance_variable_get('@buf')
            pending_outbound_data_after_draining = nc.pending_data_size
          end
        end
      end

      # Fast publisher
      nc2 = NATS.connect(uri: @s.uri) do |nc|
        inbox = NATS.create_inbox
        nc.flush do
          nc.subscribe(inbox) do |msg|
            msgs << msg
          end
        end

        timer = EM.add_periodic_timer(0.2) do
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

    # Should be the same as the messages received by the first client.
    expect(msgs.count).to eql(total_msgs_sent-extra_pubs)
    expect(msgs.count).to eql(total_msgs_received_after_drain*10)
    expect(before_publish).to eql(after_publish)

    expect(pending_outbound_data_after_draining).to eql(0) if not pending_outbound_data_after_draining.nil?
    expect(pending_data_after_draining).to eql(nil)
    expect(errors.count).to eql(0)
    expect(closed).to eql(true)
    expect(drained).to eql(true)
    expect(after_drain).to eql(false)
    expect(pending_data_after_draining).to eql(nil)
  end

  it "should support draining a connection with NATS.drain" do
    msgs = []
    drained = false
    after_drain = nil
    total_msgs_before_drain = nil
    total_msgs_after_drain = nil
    total_msgs_sent = nil
    pending_data_before_draining = nil
    pending_data_after_draining = nil
    pending_outbound_data_before_draining = nil
    pending_outbound_data_after_draining = nil

    with_em_timeout(10) do
      NATS.start(uri: @s.uri) do |nc|
        expect(nc.options[:drain_timeout]).to eql(30)

        NATS.subscribe("foo", queue: "worker") do |msg, reply|
          NATS.publish(reply, "ACK:foo")
        end

        NATS.subscribe("bar") do |msg, reply|
          NATS.publish(reply, "ACK:bar")
        end

        NATS.subscribe("quux") do |msg, reply|
          NATS.publish(reply, "ACK:quux")
        end

        EM.add_timer(1) do
          # Before draining
          pending_data_before_draining = nc.instance_variable_get('@buf')
          pending_outbound_data_before_draining = nc.pending_data_size

          total_msgs_before_drain = nc.msgs_received
          NATS.drain do
            after_drain = nc.draining?
            drained = true

            total_msgs_sent = nc.msgs_sent
            total_msgs_after_drain = nc.msgs_received
            pending_data_after_draining = nc.instance_variable_get('@buf')
            pending_outbound_data_after_draining = nc.pending_data_size
          end
        end
      end

      # Fast publisher
      NATS.connect(uri: @s.uri) do |nc|
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

    # Should be the same as the messages received by the first client.
    expect(msgs.count).to eql(total_msgs_sent)
    expect(msgs.count).to eql(total_msgs_after_drain)
    expect(pending_outbound_data_after_draining).to eql(0)
    expect(pending_data_after_draining).to eql(nil)
    expect(drained).to eql(true)
    expect(after_drain).to eql(false)
    expect(total_msgs_before_drain < total_msgs_after_drain).to eql(true)
    expect(pending_data_after_draining).to eql(nil)
  end
end
