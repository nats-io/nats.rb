
require 'spec_helper'
require 'fileutils'

describe 'Client - authorization' do
  USER = 'derek'
  PASS = 'mypassword'

  TEST_AUTH_SERVER          = "nats://#{USER}:#{PASS}@127.0.0.1:9222"
  TEST_AUTH_SERVER_NO_CRED  = 'nats://127.0.0.1:9222'
  TEST_AUTH_SERVER_PID      = '/tmp/nats_authorization.pid'
  TEST_AUTH_AUTO_SERVER_PID = '/tmp/nats_auto_authorization.pid'

  before (:each) do
    @s = NatsServerControl.new(TEST_AUTH_SERVER, TEST_AUTH_SERVER_PID)
    @s.start_server
  end

  after (:each) do
    @s.kill_server
    FileUtils.rm_f TEST_AUTH_SERVER_PID
  end

  it 'should fail to connect to an authorized server without proper credentials' do
    errors = []
    with_em_timeout do |future|
      NATS.on_error do |e|
        errors << e
      end
      NATS.connect(:uri => TEST_AUTH_SERVER_NO_CRED)
    end
    expect(errors.count).to eql(2)
    expect(errors.first).to be_a NATS::AuthError
    expect(errors.last).to be_a NATS::ConnectError
  end

  it 'should take user and password as separate options' do
    errors = []
    with_em_timeout(1) do
      NATS.on_error do |e|
        errors << e
      end
      NATS.connect(:uri => TEST_AUTH_SERVER_NO_CRED, :user => USER, :pass => PASS)
    end
    expect(errors.count).to eql(0)
  end

  it 'should not continue to try to connect on unauthorized access' do
    auth_error_callbacks = 0
    connect_error_callbacks = 0
    with_em_timeout do
      # Default error handler raises, so we trap here.
      NATS.on_error do |e|

        # disconnects
        connect_error_callbacks += 1 if e.instance_of? NATS::ConnectError

        # authorization
        auth_error_callbacks +=1 if e.instance_of? NATS::AuthError
      end

      NATS.connect(:uri => TEST_AUTH_SERVER_NO_CRED)
    end
    expect(auth_error_callbacks).to eql(1)
    expect(connect_error_callbacks).to eql(1)
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
    expect(error_cb).to eql(1)
    expect(connect_cb).to eql(true)
    expect(NATS.client).to_not be(nil)
    expect(NATS.client.server_pool.size).to eql(1)
    NATS.stop # clears err_cb
  end

end
