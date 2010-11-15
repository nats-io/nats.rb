require 'spec_helper'

describe NATSD do

  before (:all) do
    TEST_SERVER = 'nats://localhost:8222'
    @s = NatsServerControl.new(TEST_SERVER, "/tmp/nats_attack.pid")
    @s.kill_server # start fresh
    @s.start_server
  end

  it "should complain if our test server is not running" do
    NATS.start(:uri => TEST_SERVER, :autostart => false) { NATS.stop }
  end

  it "should not let us write large control line buffers" do
    BAD_BUFFER = 'a' * 1024 * 1024
    begin
      uri = URI.parse(TEST_SERVER)
      s = TCPSocket.open(uri.host, uri.port)
      lambda { s.write(BAD_BUFFER) }.should raise_error
    ensure
      s.close
    end
  end

  it "should compain if we can't kill our server because of bad pid file" do
    @s.kill_server
    received_error = false
    begin
      NATS.start(:uri => TEST_SERVER, :autostart => false) { NATS.stop }
    rescue => e
      e.should be_instance_of NATS::Error
      received_error = true
    end
    received_error.should be_true
  end

end
