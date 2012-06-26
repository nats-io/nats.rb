require 'spec_helper'

describe 'client specification' do

  before(:all) do
    USER = 'derek'
    PASS = 'mypassword'
    TEST_AUTH_SERVER = "nats://#{USER}:#{PASS}@localhost:9333"
    @as = NatsServerControl.new(TEST_AUTH_SERVER)
    @s = NatsServerControl.new
    @s.start_server
  end

  after(:all) do
    @s.kill_server
    @as.kill_server
  end

  it 'should properly report connected after connect callback' do
    NATS.start do
      NATS.connected?.should be_true
      NATS.reconnecting?.should be_false
      NATS.stop
    end
  end

  it 'should report a reconnecting event when trying to reconnect' do
    reconnect_cb = false
    NATS.start(:reconnect_time_wait => 0.25) do |c|
      timeout_nats_on_failure(1)
      c.on_reconnect do
        reconnect_cb = true
        NATS.connected?.should be_false
        NATS.reconnecting?.should be_true
        NATS.stop
      end
      @s.kill_server
    end
    reconnect_cb.should be_true
  end

  it 'should do publish without error even if reconnected to un authorized server' do
    @as.start_server
    NATS.start(:uri => TEST_AUTH_SERVER, :reconnect_time_wait => 0.25) do |c|
      c.on_reconnect do
        NATS.publish('reconnect test')
      end
      @as.kill_server
      @as.start_server
      EM.add_timer(1){
        NATS.stop
      }
    end
  end

end
