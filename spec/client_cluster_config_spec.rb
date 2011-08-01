require 'spec_helper'

describe 'client cluster config' do

  before(:all) do

    USER = 'derek'
    PASS = 'mypassword'

    CLUSTER_AUTH_PORT = 9292
    CLUSTER_AUTH_SERVER = "nats://#{USER}:#{PASS}@localhost:#{CLUSTER_AUTH_PORT}"
    CLUSTER_AUTH_SERVER_PID = '/tmp/nats_cluster_authorization.pid'

    @s = NatsServerControl.new
    @s.start_server

    @as = NatsServerControl.new(CLUSTER_AUTH_SERVER, CLUSTER_AUTH_SERVER_PID)
    @as.start_server
  end

  after(:all) do
    @s.kill_server
    @as.kill_server
  end

  it 'should properly process :uri option for multiple servers' do
    NATS.start(:uri => ['nats://127.0.0.1:4222', 'nats://127.0.0.1:4223']) do
      options = NATS.options
      options.should be_an_instance_of Hash
      options.should have_key :uri
      options[:uri].should == ['nats://127.0.0.1:4222', 'nats://127.0.0.1:4223']
      NATS.stop
    end
  end

  it 'should allow :uris and :servers as aliases' do
    NATS.start(:uris => ['nats://127.0.0.1:4222', 'nats://127.0.0.1:4223']) do
      options = NATS.options
      options.should be_an_instance_of Hash
      options.should have_key :uris
      options[:uris].should == ['nats://127.0.0.1:4222', 'nats://127.0.0.1:4223']
      NATS.stop
    end
    NATS.start(:servers => ['nats://127.0.0.1:4222', 'nats://127.0.0.1:4223']) do
      options = NATS.options
      options.should be_an_instance_of Hash
      options.should have_key :servers
      options[:servers].should == ['nats://127.0.0.1:4222', 'nats://127.0.0.1:4223']
      NATS.stop
    end
  end

  it 'should allow aliases on instance connections' do
    c1 = c2 = nil
    NATS.start do
      c1 = NATS.connect(:uris => ['nats://127.0.0.1:4222', 'nats://127.0.0.1:4223'])
      c2 = NATS.connect(:servers => ['nats://127.0.0.1:4222', 'nats://127.0.0.1:4223'])
      timeout_nats_on_failure
    end
    c1.should_not be_nil
    c2.should_not be_nil
  end

  it 'should connect to first entry' do
    NATS.start(:uri => ['nats://127.0.0.1:4222', 'nats://127.0.0.1:4223']) {  NATS.stop }
  end

  it 'should fail to connect if no servers available' do
    expect do
      NATS.start(:uri => ['nats://127.0.0.1:4223']) {  NATS.stop }
    end.to raise_error NATS::Error
  end

  it 'should connect to another server if first is not available' do
    NATS.start(:uri => ['nats://127.0.0.1:4224', 'nats://127.0.0.1:4222']) {  NATS.stop }
  end

  it 'should fail if all servers are not available' do
    expect do
      NATS.start(:uri => ['nats://127.0.0.1:4224', 'nats://127.0.0.1:4223']) {  NATS.stop }
    end.to raise_error NATS::Error
  end

  it 'should fail if server available but do not have proper auth' do
    expect do
      NATS.start(:uri => ['nats://127.0.0.1:4224', "nats://127.0.0.1:#{CLUSTER_AUTH_PORT}"]) {  NATS.stop }
    end.to raise_error NATS::Error
  end

  it 'should succeed if proper credentials supplied with non-first uri' do
    NATS.start(:uri => ['nats://127.0.0.1:4224', CLUSTER_AUTH_SERVER]) do
      NATS.client.connected_server.should == URI.parse(CLUSTER_AUTH_SERVER)
      NATS.stop
    end
  end

end
