require File.dirname(__FILE__) + '/spec_helper'

# HACK - Autostart functionality not happy with spec, so make sure running.
`ruby -S bundle exec nats-server -d`

describe NATS do

  before(:all) do
    # Thread.new { EM.run }
  end

  it 'should perform basic block start and stop' do
    NATS.start { NATS.stop }
  end

  it 'should do publish without payload and with opt_reply without error' do
    NATS.start { |nc|
      nc.publish('foo')
      nc.publish('foo', 'hello')
      nc.publish('foo', 'hello', 'reply')
      NATS.stop
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
        received=true
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
        received=true
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
        received=true
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
    new_conn.class.should == NATS
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
    new_conn.class.should == NATS
    received_request.should be_true
    received_reply.should be_true
  end

end
