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
    end.to raise_error(NATS::Error)
  end

  it 'should complain if NATS.start is called without EM running and no block was given' do
    EM.reactor_running?.should be_false
    expect { NATS.start }.to raise_error(NATS::Error)
    NATS.connected?.should be_false
  end

  it 'should perform basic block start and stop' do
    NATS.start { NATS.stop }
  end

  it 'should signal connected state' do
    NATS.start {
      NATS.connected?.should be_true
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
    received.should be_true
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
    received.should be_true
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
    received.should be_true
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
    received.should be_true
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
    received_pub_closure.should be_true
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
    received_pub_closure.should be_true
    replies.should == expected
  end

  it "should be able to start and use a new connection inside of start block" do
    new_conn = nil
    received = false
    NATS.start {
      NATS.subscribe('foo') { received = true; NATS.stop }
      new_conn = NATS.connect
      new_conn.publish('foo', 'hello')
      timeout_nats_on_failure
    }
    new_conn.should_not be_nil
    received.should be_true
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
    received_request.should be_true
    received_reply.should be_true
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
    got_error.should be_false
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
    got_error.should be_true
  end

end
