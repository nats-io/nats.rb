
require 'spec_helper'

describe 'authorization' do

  before (:all) do
    USER = 'derek'
    PASS = 'mypassword'

    TEST_AUTH_SERVER = "nats://#{USER}:#{PASS}@localhost:9222"
    TEST_AUTH_SERVER_NO_CRED = 'nats://localhost:9222'
    TEST_AUTH_SERVER_PID = '/tmp/nats_authorization.pid'

    TEST_AUTH_AUTOSTART_SERVER = "nats://#{USER}:#{PASS}@localhost:11222"
    TEST_AUTOSTART_SERVER = "nats://localhost:11222"

    @s = NatsServerControl.new(TEST_AUTH_SERVER, TEST_AUTH_SERVER_PID)
    @s.start_server
  end

  after (:all) do
    @s.kill_server if @s.was_running?
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

end
