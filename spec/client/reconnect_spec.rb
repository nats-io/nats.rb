require 'spec_helper'

describe 'Client - reconnect specification' do

  before(:all) do
    R_USER = 'derek'
    R_PASS = 'mypassword'
    R_TEST_AUTH_SERVER = "nats://#{R_USER}:#{R_PASS}@127.0.0.1:9333"
    R_TEST_SERVER_PID = '/tmp/nats_reconnect_authorization.pid'
    E_TEST_SERVER = "nats://127.0.0.1:9666"
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

  it 'should subscribe if reconnected' do
    received = false
    @as.kill_server

    EM.run do
      c = NATS.connect(:uri => R_TEST_AUTH_SERVER, :reconnect => true, :max_reconnect_attempts => -1, :reconnect_time_wait => 0.25)

      c.subscribe('foo') { |msg|
        received = true
      }

      @as.start_server

      EM.add_periodic_timer(0.1) {
        c.publish('foo', 'xxx')
      }

      EM.add_timer(1) {
        NATS.stop
        EM.stop
      }
    end

    received.should be_truthy
  end
end
