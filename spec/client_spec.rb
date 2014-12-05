require 'spec_helper'

describe 'client specification' do

  before(:all) do
    @s = NatsServerControl.new
    @s.start_server
  end

  after(:all) do
    @s.kill_server
  end

  it "should complain if it can't connect to server when not running and not told to autostart" do
    expect do
      NATS.start(:uri => 'nats://localhost:3222', :autostart => false) { NATS.stop }
    end.to raise_error(NATS::ConnectError)
  end

  it 'should complain if NATS.start is called without EM running and no block was given' do
    EM.reactor_running?.should be_falsey
    expect { NATS.start }.to raise_error(NATS::Error)
    NATS.connected?.should be_falsey
  end

  it 'should perform basic block start and stop' do
    NATS.start { NATS.stop }
  end

  it 'should signal connected state' do
    NATS.start {
      NATS.connected?.should be_truthy
      NATS.stop
    }
  end

  it 'should have err_cb cleared after stop' do
    NATS.start do
      NATS.on_error { puts 'err' }
      NATS.stop
    end
    NATS.err_cb.should be_nil
  end

  it 'should raise NATS::ServerError on error replies from NATSD' do
    expect do
      NATS.start(:pedantic => true) do
        NATS.unsubscribe(10000)
        NATS.publish('done') { NATS.stop }
      end
    end.to raise_error(NATS::ServerError)
  end

  it 'should do publish without payload and with opt_reply without error' do
    NATS.start { |nc|
      nc.publish('foo')
      nc.publish('foo', 'hello')
      nc.publish('foo', 'hello', 'reply')
      NATS.stop
    }
  end

  it 'should not complain when publishing to nil' do
    NATS.start {
      NATS.publish(nil)
      NATS.publish(nil, 'hello')
      EM.add_timer(0.1) { NATS.stop }
    }
  end

  it 'should receive a sid when doing a subscribe' do
    NATS.start { |nc|
      s = nc.subscribe('foo')
      s.should_not be_nil
      NATS.stop
    }
  end

  it 'should receive a sid when doing a request' do
    NATS.start { |nc|
      s = nc.request('foo')
      s.should_not be_nil
      NATS.stop
    }
  end

  it 'should receive a message that it has a subscription to' do
    received = false
    NATS.start { |nc|
      nc.subscribe('foo') { |msg|
        received = true
        msg.should == 'xxx'
        NATS.stop
      }
      nc.publish('foo', 'xxx')
      timeout_nats_on_failure
    }
    received.should be_truthy
  end

  it 'should receive a message that it has a wildcard subscription to' do
    received = false
    NATS.start { |nc|
      nc.subscribe('*') { |msg|
        received = true
        msg.should == 'xxx'
        NATS.stop
      }
      nc.publish('foo', 'xxx')
      timeout_nats_on_failure
    }
    received.should be_truthy
  end

  it 'should not receive a message that it has unsubscribed from' do
    received = 0
    NATS.start { |nc|
      s = nc.subscribe('*') { |msg|
        received += 1
        msg.should == 'xxx'
        nc.unsubscribe(s)
      }
      nc.publish('foo', 'xxx')
      timeout_nats_on_failure
    }
    received.should == 1
  end

  it 'should receive a response from a request' do
    received = false
    NATS.start { |nc|
      nc.subscribe('need_help') { |msg, reply|
        msg.should == 'yyy'
        nc.publish(reply, 'help')
      }
      nc.request('need_help', 'yyy') { |response|
        received=true
        response.should == 'help'
        NATS.stop
      }
      timeout_nats_on_failure
    }
    received.should be_truthy
  end

  it 'should perform similar using class mirror functions' do
    received = false
    NATS.start {
      s = NATS.subscribe('need_help') { |msg, reply|
        msg.should == 'yyy'
        NATS.publish(reply, 'help')
        NATS.unsubscribe(s)
      }
      r = NATS.request('need_help', 'yyy') { |response|
        received = true
        response.should == 'help'
        NATS.unsubscribe(r)
        NATS.stop
      }
      timeout_nats_on_failure
    }
    received.should be_truthy
  end

  it 'should return inside closure on publish when server received msg' do
    received_pub_closure = false
    NATS.start {
      NATS.publish('foo') {
        received_pub_closure = true
        NATS.stop
      }
    timeout_nats_on_failure
    }
    received_pub_closure.should be_truthy
  end

  it 'should return inside closure in ordered fashion when server received msg' do
    replies = []
    expected = []
    received_pub_closure = false
    NATS.start {
      (1..100).each { |i|
        expected << i
        NATS.publish('foo') { replies << i }
      }
      NATS.publish('foo') {
        received_pub_closure = true
        NATS.stop
      }
      timeout_nats_on_failure
    }
    received_pub_closure.should be_truthy
    replies.should == expected
  end

  it "should be able to start and use a new connection inside of start block" do
    new_conn = nil
    received = false
    NATS.start {
      NATS.subscribe('foo') { received = true; NATS.stop }
      new_conn = NATS.connect do
        new_conn.publish('foo', 'hello')
      end
      timeout_nats_on_failure(5)
    }
    new_conn.should_not be_nil
    received.should be_truthy
  end

  it 'should allow proper request/reply across multiple connections' do
    new_conn = nil
    received_request = false
    received_reply = false

    NATS.start {
      new_conn = NATS.connect
      new_conn.subscribe('test_conn_rr') do |msg, reply|
        received_request = true
        new_conn.publish(reply)
      end
      new_conn.on_connect do
        NATS.request('test_conn_rr') do
          received_reply = true
          NATS.stop
        end
      end
      timeout_nats_on_failure
    }
    new_conn.should_not be_nil
    received_request.should be_truthy
    received_reply.should be_truthy
  end

  it 'should complain if NATS.start called without a block when we would need to start EM' do
    expect do
      NATS.start
      NATS.stop
    end.to raise_error(NATS::Error)
  end

  it 'should not complain if NATS.start called without a block when EM is running already' do
    EM.run do
      expect do
        NATS.start
        EM.next_tick { NATS.stop { EM.stop } }
      end.to_not raise_error
    end
  end

  it 'should use default url if passed uri is nil' do
    NATS.start(:uri => nil) {  NATS.stop }
  end

  it 'should not complain about publish to nil unless in pedantic mode' do
    NATS.start {
      NATS.publish(nil, 'Hello!')
      NATS.stop
    }
  end

  it 'should allow proper unsubscribe from within blocks' do
    received = 0
    NATS.start do
      sid = NATS.subscribe('foo') { |msg|
        received += 1
        sid.should_not be_nil
        NATS.unsubscribe(sid)
      }
      NATS.publish('foo')
      NATS.publish('foo') { NATS.stop }
    end
    received.should == 1
  end

  it 'should not call error handler for double unsubscribe unless in pedantic mode' do
    got_error = false
    NATS.on_error { got_error = true; NATS.stop }
    NATS.start do
      s = NATS.subscribe('foo')
      NATS.unsubscribe(s)
      NATS.unsubscribe(s)
      NATS.publish('flush') { NATS.stop }
    end
    got_error.should be_falsey
  end

  it 'should call error handler for double unsubscribe if in pedantic mode' do
    got_error = false
    NATS.on_error { got_error = true; NATS.stop }
    NATS.start(:pedantic => true) do
      s = NATS.subscribe('foo')
      NATS.unsubscribe(s)
      NATS.unsubscribe(s)
      NATS.publish('flush') { NATS.stop }
    end
    got_error.should be_truthy
  end

  it 'should monitor inbound and outbound messages and bytes' do
    msg = 'Hello World!'
    NATS.start do |c|
      NATS.subscribe('foo')
      NATS.publish('foo', msg)
      NATS.publish('bar', msg)
      NATS.flush do
        c.msgs_sent.should == 2
        c.msgs_received.should == 1
        c.bytes_received.should == msg.size
        c.bytes_sent.should == msg.size * 2
        NATS.stop
      end
    end
  end

  it 'should receive a pong from a server after ping_interval' do
    NATS.start(:ping_interval => 0.25) do
      NATS.client.pongs_received.should == 0
      EM.add_timer(0.5) do
        NATS.client.pongs_received.should == 1
        NATS.stop
      end
    end
  end

  it 'should disconnect from the server when pongs not received' do
    EM.run do
      NATS.connect(:ping_interval => 0.1, :max_outstanding_pings => 1, :reconnect => false) do |c|
        c.on_error { NATS.stop }
        def c.process_pong
          # override to not process counters
        end
      end
      EM.add_timer(0.5) do
        NATS.connected?.should be_falsey
        EM.stop
      end
    end
  end

  it 'should stop the ping timer when disconnected or closed' do
    EM.run do
      $pings_received = 0
      NATS.connect(:ping_interval => 0.1) do |c|
        def c.send_ping
          $pings_received += 1
          close
        end
      end
      EM.add_timer(0.5) do
        $pings_received.should == 1
        EM.stop
      end
    end
  end

  it 'should accept the same option set twice' do
    opts = {:uri => 'nats://localhost:4222'}
    NATS.start(opts) { NATS.stop }
    NATS.start(opts) { NATS.stop }
  end

  describe '#create_inbox' do
    it 'create the expected format' do
      expect(NATS.create_inbox).to match(/_INBOX\.[a-f0-9]{12}/)
    end

    context 'when Kernel.srand is regularly reset to the same value' do
      it 'should generate a unique inbox name' do
        Kernel.srand 5555
        first_inbox_name = NATS.create_inbox

        Kernel.srand 5555
        second_inbox_name = NATS.create_inbox

        expect(second_inbox_name).to_not eq(first_inbox_name)
      end
    end
  end
end
