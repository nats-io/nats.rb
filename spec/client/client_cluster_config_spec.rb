require 'spec_helper'

describe 'Client - cluster config' do

  CLUSTER_USER = 'derek'
  CLUSTER_PASS = 'mypassword'

  CLUSTER_AUTH_PORT = 9292
  CLUSTER_AUTH_SERVER = "nats://#{CLUSTER_USER}:#{CLUSTER_PASS}@127.0.0.1:#{CLUSTER_AUTH_PORT}"
  CLUSTER_AUTH_SERVER_PID = '/tmp/nats_cluster_authorization.pid'

  before(:all) do
    @s  = NatsServerControl.new
    @as = NatsServerControl.new(CLUSTER_AUTH_SERVER, CLUSTER_AUTH_SERVER_PID)
  end

  before(:each) do
    [@s, @as].each do |s|
      s.start_server(true) unless NATS.server_running? s.uri
    end
  end

  after(:each) do
    [@s, @as].each do |s|
      s.kill_server
    end
  end

  it 'should properly process :uri option for multiple servers' do
    NATS.start(:uri => ['nats://127.0.0.1:4222', 'nats://127.0.0.1:4223'], :dont_randomize_servers => true) do
      options = NATS.options
      expect(options).to be_a(Hash)
      expect(options).to have_key(:uri)
      expect(options[:uri]).to eql(['nats://127.0.0.1:4222', 'nats://127.0.0.1:4223'])
      NATS.stop
    end
  end

  it 'should allow :uris and :servers as aliases' do
    NATS.start(:uris => ['nats://127.0.0.1:4222', 'nats://127.0.0.1:4223'], :dont_randomize_servers => true) do
      options = NATS.options
      expect(options).to be_a(Hash)
      expect(options).to have_key(:uris)
      expect(options[:uris]).to eql(['nats://127.0.0.1:4222', 'nats://127.0.0.1:4223'])
      NATS.stop
    end
    NATS.start(:servers => ['nats://127.0.0.1:4222', 'nats://127.0.0.1:4223'], :dont_randomize_servers => true) do
      options = NATS.options
      expect(options).to be_a(Hash)
      expect(options).to have_key(:servers)
      expect(options[:servers]).to eql(['nats://127.0.0.1:4222', 'nats://127.0.0.1:4223'])
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
    expect(c1).to_not be(nil)
    expect(c2).to_not be(nil)
  end

  it 'should randomize server pool list by default' do
    servers = ['nats://127.0.0.1:4222', 'nats://127.0.0.1:4223',
               'nats://127.0.0.1:4224', 'nats://127.0.0.1:4225',
               'nats://127.0.0.1:4226', 'nats://127.0.0.1:4227']
    NATS.start do
      NATS.connect(:uri => servers.dup) do |c|
        sp_servers = []
        c.server_pool.each { |s| sp_servers << s[:uri].to_s }
        expect(sp_servers).to_not eql(servers)
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
        expect(sp_servers).to eql(servers)
      end
      timeout_nats_on_failure
    end
  end

  it 'should connect to first entry if available' do
    NATS.start(:dont_randomize_servers => true, :uri => ['nats://127.0.0.1:4222', 'nats://127.0.0.1:4223']) do
      expect(NATS.client.connected_server).to eql(URI.parse('nats://127.0.0.1:4222'))
      NATS.stop
    end
  end

  it 'should fail to connect if no servers available' do
    errors = []
    with_em_timeout do
      NATS.on_error do |e|
        errors << e
      end

      NATS.start(:uri => ['nats://127.0.0.1:4223'])
    end
    expect(errors.first).to be_a(NATS::Error)
  end

  it 'should connect to another server if first is not available' do
    NATS.start(:dont_randomize_servers => true, :uri => ['nats://127.0.0.1:4224', 'nats://127.0.0.1:4222']) do
      expect(NATS.client.connected_server).to eql(URI.parse('nats://127.0.0.1:4222'))
      NATS.stop
    end
  end

  it 'should fail if all servers are not available' do
    errors = []
    with_em_timeout do
      NATS.on_error do |e|
        errors << e
      end
      NATS.connect(:uri => ['nats://127.0.0.1:4224', 'nats://127.0.0.1:4223'])
    end
    expect(errors.count > 0).to be(true)
    expect(errors.first).to     be_a(NATS::ConnectError)
  end

  it 'should fail if server is available but does not have proper auth' do
    errors = []
    with_em_timeout do
      NATS.on_error do |e|
        errors << e
      end
      NATS.connect(:uri => ['nats://127.0.0.1:4224', "nats://127.0.0.1:#{CLUSTER_AUTH_PORT}"])
    end
    expect(errors.count).to eql(2)
    expect(errors.first).to be_a(NATS::AuthError)
    expect(errors.last).to  be_a(NATS::ConnectError)
  end

  it 'should succeed if proper credentials supplied with non-first uri' do
    with_em_timeout(3) do
      # FIXME: Flush should not be required to be able to assert connected URI
      nc = NATS.connect(:dont_randomize_servers => true, :uri => ['nats://127.0.0.1:4224', CLUSTER_AUTH_SERVER])
      nc.flush do
        expect(nc.connected_server).to eql(URI.parse(CLUSTER_AUTH_SERVER))
      end
    end
  end

  it 'should allow user/pass overrides' do
    s_uri = "nats://127.0.0.1:#{CLUSTER_AUTH_PORT}"

    errors = []
    with_em_timeout(5) do
      NATS.on_error do |e|
        errors << e
      end
      NATS.connect(:servers => [s_uri])
    end
    expect(errors.count).to eql(2)
    expect(errors.first).to be_a(NATS::AuthError)
    expect(errors.last).to  be_a(NATS::ConnectError)

    errors = []
    with_em_timeout do
      NATS.connect(:uri => [s_uri], :user => CLUSTER_USER, :pass => CLUSTER_PASS) do |nc2|
        nc2.publish("hello", "world")
      end
    end
    expect(errors.count).to eql(0)
  end

  context do
    before(:all) do
      @s1_uri = 'nats://derek:foo@127.0.0.1:9290'
      @s1 = NatsServerControl.new(@s1_uri, '/tmp/nats_cluster_honor_s1.pid')
      @s1.start_server

      @s2_uri = 'nats://sarah:bar@127.0.0.1:9298'
      @s2 = NatsServerControl.new(@s2_uri, '/tmp/nats_cluster_honor_s2.pid')
      @s2.start_server
    end

    after(:all) do
      @s1.kill_server
      @s2.kill_server
    end

    it 'should honor auth credentials properly for listed servers' do
      with_em_timeout(5) do
        # FIXME: Flush should not be required, rather connect should be synchronous...
        nc = NATS.connect(:dont_randomize_servers => true, :servers => [@s1_uri, @s2_uri])
        nc.flush do
          expect(nc.connected_server).to eql(URI.parse(@s1_uri))

          # Disconnect from first server
          kill_time = Time.now
          @s1.kill_server

          EM.add_timer(1) do
            time_diff = Time.now - kill_time
            expect(time_diff < 2).to eql(true)

            # Confirm that it has connected to the second server
            expect(nc.connected_server).to eql(URI.parse(@s2_uri))

            # Restart the first server again...
            @s1.start_server

            # Kill the second server once again to reconnect to first one...
            kill_time2 = Time.now
            @s2.kill_server

            EM.add_timer(0.25) do
              time_diff = Time.now - kill_time2
              expect(time_diff < 1).to eql(true)

              # Confirm we are reconnecting to the first one again
              nc.flush do
                expect(nc.connected_server).to eql(URI.parse(@s1_uri))
              end
            end
          end
        end
      end
    end
  end
end
