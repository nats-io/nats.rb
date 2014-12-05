require 'spec_helper'

describe 'client specification' do

  before(:all) do
    R_USER = 'derek'
    R_PASS = 'mypassword'
    R_TEST_AUTH_SERVER = "nats://#{R_USER}:#{R_PASS}@localhost:9333"
    R_TEST_SERVER_PID = '/tmp/nats_reconnect_authorization.pid'
    E_TEST_SERVER = "nats://localhost:9666"
    E_TEST_SERVER_PID = '/tmp/nats_reconnect_exception_test.pid'

    @as = NatsServerControl.new(R_TEST_AUTH_SERVER, R_TEST_SERVER_PID)
    @as.start_server
    @s = NatsServerControl.new
    @s.start_server
    @es = NatsServerControl.new(E_TEST_SERVER, E_TEST_SERVER_PID)
    @es.start_server
  end

  after(:all) do
    @s.kill_server
    @as.kill_server
    @es.kill_server
  end

  it 'should properly report connected after connect callback' do
    NATS.start do
      NATS.connected?.should be_truthy
      NATS.reconnecting?.should be_falsey
      NATS.stop
    end
  end

  it 'should properly handle exceptions thrown by eventmachine during reconnects' do
    reconnect_cb = false
    NATS.start(:uri => E_TEST_SERVER, :reconnect_time_wait => 0.25) do |c|
      # Change the uri to simulate a DNS failure which will make EM.reconnect throw an exception
      c.instance_eval('@uri = URI.parse("nats://does.not.exist:4222/")')
      timer=timeout_nats_on_failure(1)
      c.on_reconnect do
        reconnect_cb = true
        NATS.connected?.should be_falsey
        NATS.reconnecting?.should be_truthy
        NATS.stop
      end
      @es.kill_server
    end
    reconnect_cb.should be_truthy
  end

  it 'should report a reconnecting event when trying to reconnect' do
    reconnect_cb = false
    NATS.start(:reconnect_time_wait => 0.25) do |c|
      timeout_nats_on_failure(1)
      c.on_reconnect do
        reconnect_cb = true
        NATS.connected?.should be_falsey
        NATS.reconnecting?.should be_truthy
        NATS.stop
      end
      @s.kill_server
    end
    reconnect_cb.should be_truthy
    @s.start_server
  end

  it 'should allow binding of callback on default client after initialization' do
    reconnect_cb = false
    NATS.start(:reconnect_time_wait => 0.25) do |c|
      timeout_nats_on_failure(1)
      NATS.on_reconnect do
        reconnect_cb = true
        NATS.connected?.should be_falsey
        NATS.reconnecting?.should be_truthy
        NATS.stop
      end
      @s.kill_server
    end
    reconnect_cb.should be_truthy
  end

  it 'should do publish without error even if reconnected to an authorized server' do
    NATS.start(:uri => R_TEST_AUTH_SERVER, :reconnect_time_wait => 0.25) do |c|
      c.on_reconnect do
        expect do
          NATS.publish('reconnect test')
        end.to_not raise_error
      end
      @as.kill_server
      EM.add_timer(0.25) { @as.start_server }
      EM.add_timer(1.0) { NATS.stop }
    end
  end

end
