
require 'spec_helper'

describe 'autostart' do

  before (:all) do
    TEST_AUTO_START_SERVER = 'nats://localhost:9229'
    @s = NatsServerControl.new(TEST_AUTO_START_SERVER, "/tmp/nats_test_auto_start.pid")
    @s.start_server
  end

  after (:all) do
    @s.kill_server if @s.was_running?
  end

  it 'should not autostart a server if no flag is set' do
    expect do
      NATS.start(:uri => TEST_SERVER) { NATS.stop }
    end.to raise_error NATS::Error
  end

  it 'should autostart a server if requested' do
    received = 0
    expect do
      NATS.start(:uri => TEST_SERVER, :autostart => true) {
        NATS.subscribe('foo') { received += 1 }
        NATS.publish('foo') {  NATS.stop }
      }
    end.to_not raise_error NATS::Error
    received.should == 1
  end

end
