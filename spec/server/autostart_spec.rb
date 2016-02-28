
require 'spec_helper'

describe 'autostart' do

  before (:all) do
    AUTO_START_SERVER = 'nats://localhost:9229'
    @s = NatsServerControl.new(AUTO_START_SERVER, NATS::AUTOSTART_PID_FILE)
  end

  after (:each) do
    @s.kill_server
  end

  it 'should not autostart a server if no flag is set' do
    expect do
      NATS.start(:uri => AUTO_START_SERVER) { NATS.stop }
    end.to raise_error NATS::ConnectError
  end

  it 'should autostart a server if requested' do
    received = 0
    expect do
      NATS.start(:uri => AUTO_START_SERVER, :autostart => true) {
        NATS.subscribe('foo') { received += 1 }
        NATS.publish('foo') {  NATS.stop }
      }
    end.to_not raise_error
    received.should == 1
  end

end
