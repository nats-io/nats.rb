require 'spec_helper'

describe 'Client - server_info support' do

  before(:all) do
    @s = NatsServerControl.new
    @s.start_server
  end

  after(:all) do
    @s.kill_server
  end

  it 'should report nil when not connected for server_info' do
    expect(NATS.connected?).to eql(false)
    expect(NATS.server_info).to eql(nil)
  end

  it 'should report the appropriate server_info for connected clients' do
    NATS.start do
      info = NATS.server_info
      expect(info).to_not eql(nil)
      expect(info).to be_a Hash
      expect(info).to have_key :server_id
      expect(info).to have_key :version
      expect(info).to have_key :auth_required
      expect(info).to have_key :tls_required
      expect(info).to have_key :max_payload
      expect(info).to have_key :host
      expect(info).to have_key :port
      NATS.stop
    end
  end

  it 'should report server info for individual connections' do
    NATS.start do
      info = NATS.client.server_info
      expect(info).to_not be(nil)
      expect(info).to be_a Hash
      expect(info).to have_key :server_id
      expect(info).to have_key :version
      expect(info).to have_key :auth_required
      expect(info).to have_key :tls_required
      expect(info).to have_key :max_payload
      expect(info).to have_key :host
      expect(info).to have_key :port
      NATS.stop
    end
  end
end
