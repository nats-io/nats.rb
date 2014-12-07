
require 'spec_helper'

describe 'server attacks' do

  before (:all) do
    TEST_SERVER = 'nats://localhost:8222'
    @s = NatsServerControl.new(TEST_SERVER, "/tmp/nats_attack.pid")
    @s.start_server
  end

  after (:all) do
    @s.kill_server unless @s.was_running?
  end

  it "should complain if our test server is not running" do
    NATS.start(:uri => TEST_SERVER, :autostart => false) { NATS.stop }
  end

  it "should not let us write large control line buffers" do
    BAD_BUFFER = 'a' * 10 * 1024 * 1024
    begin
      uri = URI.parse(TEST_SERVER)
      s = TCPSocket.open(uri.host, uri.port)
      expect { s.write(BAD_BUFFER) }.to raise_error
    ensure
      s.close if s
    end
  end

  it "should not let us write large messages" do
    BIG_MSG = 'a' * 10 * 1024 * 1024
    expect do
      NATS.start(:uri => TEST_SERVER, :autostart => false, :reconnect => false) do
        NATS.publish('foo', BIG_MSG) { EM.stop }
      end
    end.to raise_error NATS::ServerError
    NATS.connected?.should == false
  end

  it "should complain if we can't kill our server that we started" do
    unless @s.was_running?
      @s.kill_server
      expect do
        NATS.start(:uri => TEST_SERVER, :autostart => false) { NATS.stop }
      end.to raise_error NATS::ConnectError
    end
  end

end
