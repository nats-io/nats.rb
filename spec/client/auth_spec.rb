
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
    end.to_not raise_error
  end

  it 'should not continue to try to connect on unauthorized access' do
    auth_error_callbacks = 0
    connect_error_callbacks = 0
    EM.run do
      # Default error handler raises, so we trap here.
      NATS.on_error do |e|
        # disconnects
        connect_error_callbacks += 1 if e.instance_of? NATS::ConnectError
        # authorization
        auth_error_callbacks +=1 if e.instance_of? NATS::AuthError
      end

      NATS.connect(:uri => TEST_AUTH_SERVER_NO_CRED)
      # Time limit bad behavior
      EM.add_timer(0.25) do
        NATS.stop
        EM.next_tick { EM.stop }
      end
    end
    auth_error_callbacks.should == 1
    connect_error_callbacks.should == 1
  end

  it 'should remove server from the pool on unauthorized access' do
    error_cb = 0
    connect_cb = false
    EM.run do
      # Default error handler raises, so we trap here.
      NATS.on_error { error_cb += 1 }
      connected = false
      NATS.start(:dont_randomize_servers => true, :servers => [TEST_AUTH_SERVER_NO_CRED, TEST_AUTH_SERVER]) do
        connect_cb = true
        EM.stop
      end
    end
    error_cb.should == 1
    connect_cb.should be_truthy
    NATS.client.should_not == nil
    NATS.client.server_pool.size.should == 1
    NATS.stop # clears err_cb
  end

end
