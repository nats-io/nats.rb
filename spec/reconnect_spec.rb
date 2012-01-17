require 'spec_helper'

describe 'client specification' do

  before(:all) do
    @s = NatsServerControl.new
    @s.start_server
  end

  after(:all) do
    @s.kill_server
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

end
