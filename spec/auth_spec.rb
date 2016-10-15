require 'spec_helper'
require 'fileutils'

describe 'Client - Authorization' do

  USER = 'secret'
  PASS = 'password'

  TEST_AUTH_SERVER          = "nats://#{USER}:#{PASS}@127.0.0.1:9222"
  TEST_AUTH_SERVER_PID      = '/tmp/nats_authorization.pid'
  TEST_AUTH_SERVER_NO_CRED  = 'nats://127.0.0.1:9222'

  TEST_ANOTHER_AUTH_SERVER  = "nats://#{USER}:secret@127.0.0.1:9223"  
  TEST_ANOTHER_AUTH_SERVER_PID = '/tmp/nats_another_authorization.pid'

  before (:each) do
    @s1 = NatsServerControl.new(TEST_AUTH_SERVER, TEST_AUTH_SERVER_PID)
    @s1.start_server

    @s2 = NatsServerControl.new(TEST_AUTH_SERVER, TEST_AUTH_SERVER_PID)
    @s2.start_server
  end

  after (:each) do
    @s1.kill_server
    FileUtils.rm_f TEST_AUTH_SERVER_PID
  end

  it 'should connect to an authorized server with proper credentials' do
    nats = NATS::IO::Client.new
    expect do
      nats.connect(:servers => [TEST_AUTH_SERVER])
    end.to_not raise_error
  end
  
  it 'should fail to connect to an authorized server without proper credentials' do
    nats = NATS::IO::Client.new
    expect do
      nats.connect(:servers => [TEST_AUTH_SERVER_NO_CRED])
    end.to raise_error(NATS::IO::AuthError)
  end
end
