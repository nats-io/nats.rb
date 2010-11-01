require File.dirname(__FILE__) + '/spec_helper'

# HACK - Autostart functionality not happy with spec, so make sure running.
`ruby -S bundle exec nats-server -d`

describe NATS do

  before(:all) do
    Thread.new { EM.run }
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
      nc.subscribe('foo') { |sub, msg|
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
      nc.subscribe('*') { |sub, msg|
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
      s = nc.subscribe('*') { |sub, msg|
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
      nc.subscribe('need_help') { |sub, msg, reply|
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

end
