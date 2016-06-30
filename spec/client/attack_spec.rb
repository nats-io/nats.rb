
require 'spec_helper'

describe 'Client - server attacks' do

  TEST_SERVER = 'nats://127.0.0.1:4222'
  MSG_SIZE    = 10 * 1024 * 1024
  BIG_MSG     = 'a' * MSG_SIZE
  BAD_SUBJECT = 'b' * 1024 * 1024

  before (:each) do
    @s = NatsServerControl.new(TEST_SERVER, "/tmp/nats_attack.pid")
    @s.start_server
  end

  after (:each) do
    @s.kill_server
  end

  it "should not let us write large control line buffers" do
    skip 'FIXME: add control line checks in server'

    errors = []
    with_em_timeout(3) do
      NATS.on_error do |e|
        errors << e
      end
      nc = NATS.connect(:servers => [TEST_SERVER], :reconnect => false)
      nc.flush do
        nc.publish(BAD_SUBJECT, 'a')
      end
    end

    expect(NATS.connected?).to be false
    expect(errors.count).to eql(2)
    expect(errors[0]).to be_a NATS::ServerError
    expect(errors[1]).to be_a NATS::ConnectError
  end

  it "should not let us write large messages" do
    errors = []
    with_em_timeout(3) do
      NATS.on_error do |e|
        errors << e
      end
      nc = NATS.connect(:uri => TEST_SERVER, :reconnect => false)
      nc.flush do
        nc.publish('foo', BIG_MSG)
      end
    end
    expect(errors.count > 0).to be true

    # NOTE: Race here on whether getting NATS::ServerError or NATS::ConnectError
    # in case we have been disconnected before reading the error sent by server.
    case errors.count
    when 1
      expect(errors[0]).to be_a NATS::ConnectError
    when 2
      expect(errors[0]).to be_a NATS::ServerError
      expect(errors[1]).to be_a NATS::ConnectError
    end
  end

  it "should complain if we can't kill our server that we started" do
    unless @s.was_running?
      @s.kill_server
      expect do
        NATS.start(:uri => TEST_SERVER) { NATS.stop }
      end.to raise_error NATS::ConnectError
    end
  end
end
