require 'spec_helper'

describe 'Client - Specification' do

  before(:each) do
    @s = NatsServerControl.new
    @s.start_server(true)
  end

  after(:each) do
    @s.kill_server
  end

  it 'should process errors from server' do
    nats = NATS::IO::Client.new
    nats.connect

    errors = []
    nats.on_error do |e|
      errors << e
    end

    # Trigger invalid subject server error which the client
    # detects so that it will disconnect
    nats.subscribe("hello.")

    # FIXME: This can fail due to timeout because
    # disconnection may have already occurred.
    nats.flush(1) rescue nil

    # Should have a connection closed at this without reconnecting.
    expect(nats.closed?).to eql(true)
    expect(errors.count).to eql(1)
    expect(errors.first).to be_a(NATS::IO::ServerError)
  end

  it 'should handle unknown errors in the protocol' do
    mon = Monitor.new
    done = mon.new_cond

    nats = NATS::IO::Client.new
    nats.connect

    errors = []
    nats.on_error do |e|
      errors << e
    end

    disconnects = 0
    nats.on_disconnect do
      disconnects += 1
    end

    closes = 0
    nats.on_close do
      closes += 1
      mon.synchronize do
        done.signal
      end
    end

    # Modify state from internal parser
    parser = nats.instance_variable_get("@parser")
    parser.parse("ASDF\r\n")
    mon.synchronize do
      done.wait(1)
    end
    expect(errors.count).to eql(1)
    expect(errors.first).to be_a(NATS::IO::ServerError)
    expect(errors.first.to_s).to include("Unknown protocol")
    expect(disconnects).to eql(1)
    expect(closes).to eql(1)
    expect(nats.closed?).to eql(true)
  end
end
