
require 'spec_helper'
require 'fileutils'

describe 'authorization' do

  before (:all) do
    USER = 'derek'
    PASS = 'mypassword'

    TEST_AUTH_SERVER = "nats://#{USER}:#{PASS}@localhost:9222"
    TEST_AUTH_SERVER_NO_CRED = 'nats://localhost:9222'
    TEST_AUTH_SERVER_PID = '/tmp/nats_authorization.pid'
    TEST_AUTH_AUTO_SERVER_PID = '/tmp/nats_auto_authorization.pid'

    TEST_AUTH_AUTOSTART_SERVER = "nats://#{USER}:#{PASS}@localhost:11222"
    TEST_AUTOSTART_SERVER = "nats://localhost:11222"

    @as = NatsServerControl.new(TEST_AUTH_AUTOSTART_SERVER, TEST_AUTH_AUTO_SERVER_PID)

    @s = NatsServerControl.new(TEST_AUTH_SERVER, TEST_AUTH_SERVER_PID)
    @s.start_server
  end

  after (:all) do
    @as.kill_server
    @s.kill_server
    FileUtils.rm_f TEST_AUTH_SERVER_PID
  end

  it 'should fail to connect to an authorized server without proper credentials' do
    expect do
      NATS.start(:uri => TEST_AUTH_SERVER_NO_CRED) { NATS.stop }
    end.to raise_error NATS::Error
  end

  it 'should autostart an authorized server correctly' do
    expect do
      NATS.start(:uri => TEST_AUTH_AUTOSTART_SERVER, :autostart => true) { NATS.stop }
    end.to_not raise_error

    expect do
      NATS.start(:uri => TEST_AUTOSTART_SERVER) { NATS.stop }
    end.to raise_error NATS::Error

    NatsServerControl.kill_autostart_server
  end

  it 'should take user and password as separate options' do
    expect do
      NATS.start(:uri => TEST_AUTH_SERVER_NO_CRED, :user => USER, :pass => PASS) { NATS.stop }
    end.to_not raise_error NATS::Error
  end

  it 'should include the user in published messages' do
    received = nil
    NATS.start(:uri => TEST_AUTH_SERVER) { |nc|
      nc.subscribe('foo') { |msg, reply, subject, user|
        received = true
        msg.should == 'xxx'
        subject.should == 'foo'
        user.should == USER
        NATS.stop
      }
      nc.publish('foo', 'xxx')
      timeout_nats_on_failure
    }
    received.should be_true
  end


  it 'should include the repsonding user from a request' do
    received = false
    NATS.start(:uri => TEST_AUTH_SERVER) { |nc|
      nc.subscribe('need_help') { |msg, reply|
        msg.should == 'yyy'
        nc.publish(reply, 'help')
      }
      nc.request('need_help', 'yyy') { |response, reply, user|
        received=true
        response.should == 'help'
        user.should == USER
        NATS.stop
      }
      timeout_nats_on_failure
    }
    received.should be_true
  end


end
