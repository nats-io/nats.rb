
require 'spec_helper'

describe 'Client - server attacks' do

  TEST_SERVER = 'nats://127.0.0.1:4222'
  MSG_SIZE    = 10 * 1024 * 1024
  BIG_MSG     = 'a' * MSG_SIZE
  BAD_SUBJECT = 'b' * 1024 * 1024

  before (:each) do
    @s = NatsServerControl.new(TEST_SERVER, "/tmp/nats_attack.pid")
    @s.start_server(true)
  end

  after (:each) do
    @s.kill_server
  end

  it "should not let us write large control line buffers" do
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

    # Just confirm that it is no longer connected
    expect(NATS.connected?).to be false
    expect(errors.count >= 1).to eql(true)
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
    errors = []
    @s.kill_server
    with_em_timeout(5) do
      NATS.on_error do |e|
        errors << e
      end
      NATS.connect(:uri => TEST_SERVER)
    end
    expect(errors.first).to be_a(NATS::ConnectError)
  end
end
