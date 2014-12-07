require 'spec_helper'

describe 'client cluster config' do

  before(:all) do

    CLUSTER_USER = 'derek'
    CLUSTER_PASS = 'mypassword'

    CLUSTER_AUTH_PORT = 9292
    CLUSTER_AUTH_SERVER = "nats://#{CLUSTER_USER}:#{CLUSTER_PASS}@localhost:#{CLUSTER_AUTH_PORT}"
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
    NATS.start(:uri => ['nats://127.0.0.1:4222', 'nats://127.0.0.1:4223'], :dont_randomize_servers => true) do
      options = NATS.options
      options.should be_an_instance_of Hash
      options.should have_key :uri
      options[:uri].should == ['nats://127.0.0.1:4222', 'nats://127.0.0.1:4223']
      NATS.stop
    end
  end

  it 'should allow :uris and :servers as aliases' do
    NATS.start(:uris => ['nats://127.0.0.1:4222', 'nats://127.0.0.1:4223'], :dont_randomize_servers => true) do
      options = NATS.options
      options.should be_an_instance_of Hash
      options.should have_key :uris
      options[:uris].should == ['nats://127.0.0.1:4222', 'nats://127.0.0.1:4223']
      NATS.stop
    end
    NATS.start(:servers => ['nats://127.0.0.1:4222', 'nats://127.0.0.1:4223'], :dont_randomize_servers => true) do
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
      c1 = NATS.connect(:uris => ['nats://127.0.0.1:4222', 'nats://127.0.0.1:4222'])
      c2 = NATS.connect(:servers => ['nats://127.0.0.1:4222', 'nats://127.0.0.1:4222'])
      timeout_nats_on_failure
    end
    c1.should_not be_nil
    c2.should_not be_nil
  end

  it 'should randomize server pool list by default' do
    servers = ['nats://127.0.0.1:4222', 'nats://127.0.0.1:4223',
               'nats://127.0.0.1:4224', 'nats://127.0.0.1:4225',
               'nats://127.0.0.1:4226', 'nats://127.0.0.1:4227']
    NATS.start do
      NATS.connect(:uri => servers.dup) do |c|
        sp_servers = []
        c.server_pool.each { |s| sp_servers << s[:uri].to_s }
        sp_servers.should_not == servers
      end
      timeout_nats_on_failure
    end
  end

  it 'should not randomize server pool if options suppress' do
    servers = ['nats://127.0.0.1:4222', 'nats://127.0.0.1:4223',
               'nats://127.0.0.1:4224', 'nats://127.0.0.1:4225',
               'nats://127.0.0.1:4226', 'nats://127.0.0.1:4227']
    NATS.start do
      NATS.connect(:dont_randomize_servers => true, :uri => servers) do |c|
        sp_servers = []
        c.server_pool.each { |s| sp_servers << s[:uri].to_s }
        sp_servers.should == servers
        NATS.stop
      end
      timeout_nats_on_failure
    end
  end

  it 'should connect to first entry if available' do
    NATS.start(:dont_randomize_servers => true, :uri => ['nats://127.0.0.1:4222', 'nats://127.0.0.1:4223']) do
      NATS.client.connected_server.should == URI.parse('nats://127.0.0.1:4222')
      NATS.stop
    end
  end

  it 'should fail to connect if no servers available' do
    expect do
      NATS.start(:uri => ['nats://127.0.0.1:4223']) { NATS.stop }
    end.to raise_error NATS::Error
  end

  it 'should connect to another server if first is not available' do
    NATS.start(:dont_randomize_servers => true, :uri => ['nats://127.0.0.1:4224', 'nats://127.0.0.1:4222']) do
      NATS.client.connected_server.should == URI.parse('nats://127.0.0.1:4222')
      NATS.stop
    end
  end

  it 'should fail if all servers are not available' do
    expect do
      NATS.start(:uri => ['nats://127.0.0.1:4224', 'nats://127.0.0.1:4223']) { NATS.stop }
    end.to raise_error NATS::Error
  end

  it 'should fail if server is available but does not have proper auth' do
    expect do
      NATS.start(:uri => ['nats://127.0.0.1:4224', "nats://127.0.0.1:#{CLUSTER_AUTH_PORT}"]) { NATS.stop }
    end.to raise_error NATS::Error
  end

  it 'should succeed if proper credentials supplied with non-first uri' do
    NATS.start(:dont_randomize_servers => true, :uri => ['nats://127.0.0.1:4224', CLUSTER_AUTH_SERVER]) do
      NATS.client.connected_server.should == URI.parse(CLUSTER_AUTH_SERVER)
      NATS.stop
    end
  end

  it 'should honor auth credentials properly for listed servers' do
    s1_uri = 'nats://derek:foo@localhost:9290'
    s1 = NatsServerControl.new(s1_uri, '/tmp/nats_cluster_s1.pid')
    s1.start_server

    s2_uri = 'nats://sarah:bar@localhost:9298'
    s2 = NatsServerControl.new(s2_uri, '/tmp/nats_cluster_s2.pid')
    s2.start_server

    NATS.start(:dont_randomize_servers => true, :uri => [s1_uri, s2_uri]) do
      NATS.client.connected_server.should == URI.parse(s1_uri)
      kill_time = Time.now
      s1.kill_server
      EM.add_timer(0.25) do
        time_diff = Time.now - kill_time
        time_diff.should < 1
        NATS.client.connected_server.should == URI.parse(s2_uri)
        s1.start_server
        kill_time2 = Time.now
        s2.kill_server
        EM.add_timer(0.25) do
          time_diff = Time.now - kill_time2
          time_diff.should < 1
          NATS.client.connected_server.should == URI.parse(s1_uri)
          NATS.stop
        end
      end
    end
    s1.kill_server
    s2.kill_server
  end

  it 'should allow user/pass overrides' do
    s_uri = "nats://localhost:#{CLUSTER_AUTH_PORT}"

    expect do
      NATS.start(:uri => [s_uri]) { NATS.stop }
    end.to raise_error NATS::Error

    expect do
      NATS.start(:uri => [s_uri], :user => CLUSTER_USER, :pass => CLUSTER_PASS) { NATS.stop }
    end.to_not raise_error
  end

end
