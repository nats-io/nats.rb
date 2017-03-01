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
      expect(NATS.connected?).to eql(true)
      expect(NATS.reconnecting?).to eql(false)
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

    expect(received).to eql(true)
  end

  it 'should not get stuck reconnecting due to uncaught exceptions' do
    received = false
    @as.kill_server

    class SomeException < StandardError; end

    expect do
      EM.run do
        NATS.connect(:uri => R_TEST_AUTH_SERVER, :reconnect => true, :max_reconnect_attempts => -1, :reconnect_time_wait => 0.25)
        raise SomeException.new
      end
    end.to raise_error(SomeException)
  end

  it 'should back off trying to reconnect' do
    @as.start_server

    disconnected_time = nil
    reconnected_time = nil
    connected_once = false
    EM.run do
      NATS.on_disconnect do
        # Capture the time of the first disconnect
        disconnected_time ||= NATS::MonotonicTime.now
      end

      NATS.on_reconnect do
        reconnected_time ||= NATS::MonotonicTime.now
      end

      NATS.connect(:uri => R_TEST_AUTH_SERVER, :reconnect => true, :max_reconnect_attempts => -1, :reconnect_time_wait => 2) do
        connected_once = true
      end

      EM.add_timer(0.5) do
        @as.kill_server
      end

      EM.add_timer(1) do
        @as.start_server
      end

      EM.add_timer(3) do
        NATS.stop
        EM.stop
      end
    end

    expect(connected_once).to eql(true)
    expect(disconnected_time).to_not be(nil)
    expect(reconnected_time).to_not be(nil)
    expect(reconnected_time - disconnected_time >= 2).to eql(true)
  end
end
