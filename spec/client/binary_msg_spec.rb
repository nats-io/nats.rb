require 'spec_helper'

describe 'test binary message payloads to avoid connection drop' do

  before(:all) do
    @s = NatsServerControl.new
    @s.start_server
  end

  after(:all) do
    @s.kill_server
  end

  it "should not disconnect us if we send binary data" do
    got_error = false
    NATS.on_error { got_error = true; NATS.stop }
    NATS.start(:reconnect => false) do
      NATS.connected?.should == true
      NATS.publish('dont_disconnect_me', "\006")
      NATS.flush { NATS.stop }
    end
    got_error.should == false
  end

end
