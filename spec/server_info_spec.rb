require 'spec_helper'

describe 'server_info support' do

  before(:all) do
    @s = NatsServerControl.new
    @s.start_server
  end

  after(:all) do
    @s.kill_server
  end

  it 'should report nil when not connected for server_info' do
    NATS.connected?.should be_falsey
    NATS.server_info.should be_nil
  end

  it 'should report the appropriate server_info for connected clients' do
    NATS.start do
      info = NATS.server_info
      info.should_not be_nil
      info.should be_an_instance_of Hash
      info.should have_key :server_id
      info.should have_key :version
      info.should have_key :auth_required
      info.should have_key :ssl_required
      info.should have_key :max_payload
      info.should have_key :host
      info.should have_key :port
      NATS.stop
    end
  end

  it 'should report server info for individual connections' do
    NATS.start do
      info = NATS.client.server_info
      info.should_not be_nil
      info.should be_an_instance_of Hash
      info.should have_key :server_id
      info.should have_key :version
      info.should have_key :auth_required
      info.should have_key :ssl_required
      info.should have_key :max_payload
      info.should have_key :host
      info.should have_key :port
      NATS.stop
    end
  end

end
