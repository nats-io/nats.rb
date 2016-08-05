require 'spec_helper'
require 'yaml'

describe 'Client - cluster auto discovery' do

  before(:each) do

    auth_options = {
      'user'     => 'secret',
      'password' => 'user',
      'token'    => 'deadbeef',
      'timeout'  => 5
    }

    s1_config_opts = {
      'pid_file'      => '/tmp/nats_cluster_s1.pid',
      'authorization' => auth_options,
      'host'          => '127.0.0.1',
      'port'          => 4242,
      'cluster_port'  => 6222
    }

    s2_config_opts = {
      'pid_file'      => '/tmp/nats_cluster_s2.pid',
      'authorization' => auth_options,
      'host'          => '127.0.0.1',
      'port'          => 4243,
      'cluster_port'  => 6223
    }

    s3_config_opts = {
      'pid_file'      => '/tmp/nats_cluster_s3.pid',
      'authorization' => auth_options,
      'host'          => '127.0.0.1',
      'port'          => 4244,
      'cluster_port'  => 6224
    }

    s4_config_opts = {
      'pid_file'      => '/tmp/nats_cluster_s4.pid',
      'authorization' => auth_options,
      'host'          => '127.0.0.1',
      'port'          => 4245,
      'cluster_port'  => 6225
    }

    s5_config_opts = {
      'pid_file'      => '/tmp/nats_cluster_s5.pid',
      'authorization' => auth_options,
      'host'          => '127.0.0.1',
      'port'          => 4246,
      'cluster_port'  => 6226
    }

    nodes = []
    configs = [s1_config_opts, s2_config_opts, s3_config_opts, s4_config_opts, s5_config_opts]
    configs.each do |config_opts|

      other_nodes_configs = configs.select do |conf|
        conf['cluster_port'] != config_opts['cluster_port']
      end

      routes = []
      other_nodes_configs.each do |conf|
        routes <<  "nats-route://foo:bar@127.0.0.1:#{conf['cluster_port']}"
      end

      nodes << NatsServerControl.init_with_config_from_string(%Q(
        host: '#{config_opts['host']}'
        port:  #{config_opts['port']}

        pid_file: '#{config_opts['pid_file']}'

        authorization {
          user: '#{auth_options["user"]}'
          password: '#{auth_options["password"]}'
          timeout: 5
        }

        cluster {
          host: '#{config_opts['host']}'
          port: #{config_opts['cluster_port']}

          authorization {
            user: foo
            password: bar
            timeout: 5
          }

          routes = [
            #{routes.join("\n            ")}
          ]
        }
      ), config_opts)
    end

    @s1, @s2, @s3, @s4, @s5 = nodes
  end

  after(:each) do
    [@s1, @s2, @s3, @s4, @s5].each do |s|
      s.kill_server if NATS.server_running?(s.uri)
    end
  end

  it 'should properly discover nodes in cluster upon connect and randomize by default' do
    # Start servers and form a cluster, client will only be aware of first node
    # though it will discover the other nodes in the cluster automatically.
    [@s1, @s2, @s3].each do |node|
      node.start_server(true)
    end

    servers_upon_connect = 0
    c1_msgs = []

    randomize_calls = []
    allow_any_instance_of(Array).to receive(:'shuffle!') do |srvs|
      randomize_calls << srvs
    end

    with_em_timeout do
      c1 = NATS.connect(:servers => [@s1.uri]) do |nats|
        servers_upon_connect = nats.server_pool.count
        expect(nats.server_pool.first[:uri]).to eql(nats.connected_server)
        NATS.stop
      end

      c1.subscribe("hello") do |msg|
        c1_msgs << msg
      end
    end
    expect(servers_upon_connect).to eql(3)
    srvs = randomize_calls.last
    expect(srvs.count).to eql(2)
    expect(srvs[0][:uri]).to eql(@s2.uri)
    expect(srvs[1][:uri]).to eql(@s3.uri)
  end

  it 'should properly discover nodes in cluster eventually after first connect' do
    [@s1, @s2].each do |node|
      node.start_server(true)
    end

    servers_upon_connect = 0
    servers_after_connect = 0
    with_em_timeout(5) do
      c1 = NATS.connect(:servers => [@s1.uri]) do |nats|
        servers_upon_connect = nats.server_pool.count
        expect(nats.connected_server).to eql(@s1.uri)
        expect(nats.server_pool.first[:uri]).to eql(nats.connected_server)

        @s3.start_server(true)
        EM.add_timer(2) do
          # Should have detected new server asynchronously
          servers_after_connect = nats.server_pool.count
          expect(nats.server_pool.first[:uri]).to eql(nats.connected_server)
        end
      end
    end
    expect(servers_upon_connect).to eql(2)
    expect(servers_after_connect).to eql(3)
  end

  it 'should properly discover nodes in cluster eventually' do
    [@s1, @s2, @s3, @s4, @s5].each do |s|
      s.start_server(true)
    end
    pool_a = nil
    pool_b = nil
    pool_c = nil
    c1_errors = []
    c2_errors = []
    c3_errors = []

    with_em_timeout(5) do
      c1 = NATS.connect(:servers => [@s1.uri], :reconnect => false) do |nats|
        expect(nats.connected_server).to eql(@s1.uri)
        expect(nats.server_pool.first[:uri]).to eql(nats.connected_server)

        EM.add_timer(2) do
          pool_a = nats.server_pool
          expect(nats.connected_server).to eql(@s1.uri)
          expect(nats.server_pool.first[:uri]).to eql(nats.connected_server)
          c1.close
        end
      end
      c1.on_error do |e|
        c1_errors << e
      end

      c2 = NATS.connect(:servers => [@s1.uri], :reconnect => false, :dont_randomize_servers => false) do |nats|
        expect(nats.connected_server).to eql(@s1.uri)
        expect(nats.server_pool.first[:uri]).to eql(nats.connected_server)

        EM.add_timer(2) do
          pool_b = nats.server_pool
          expect(nats.connected_server).to eql(@s1.uri)
          expect(nats.server_pool.first[:uri]).to eql(nats.connected_server)
          c2.close
        end
      end
      c2.on_error do |e|
        c2_errors << e
      end

      c3 = NATS.connect(:servers => [@s1.uri], :reconnect => false) do |nats|
        expect(nats.connected_server).to eql(@s1.uri)
        expect(nats.server_pool.first[:uri]).to eql(nats.connected_server)

        EM.add_timer(2) do
          pool_c = nats.server_pool
          expect(nats.connected_server).to eql(@s1.uri)
          expect(nats.server_pool.first[:uri]).to eql(nats.connected_server)
          c3.close
        end
      end
      c3.on_error do |e|
        c3_errors << e
      end
    end
    expect(pool_a.count).to eql(5)
    expect(pool_b.count).to eql(5)
    expect(pool_c.count).to eql(5)
    expect(c1_errors.count).to eql(0)
    expect(c2_errors.count).to eql(0)
    expect(c3_errors.count).to eql(0)

    # Uncommented as this can flap
    #
    # a = (pool_a[1][:uri].port == pool_c[1][:uri].port) && (pool_b[1][:uri].port == pool_c[1][:uri].port)
    # b = (pool_a[2][:uri].port == pool_c[2][:uri].port) && (pool_b[2][:uri].port == pool_c[2][:uri].port)
    # c = (pool_a[3][:uri].port == pool_c[3][:uri].port) && (pool_b[3][:uri].port == pool_c[3][:uri].port)
    # d = (pool_a[4][:uri].port == pool_c[4][:uri].port) && (pool_b[4][:uri].port == pool_c[4][:uri].port)
    # expect(a && b && c && d).to eql(false)
  end

  it 'should properly discover nodes in cluster and reconnect to new one on failure' do
    [@s1, @s2].each do |node|
      node.start_server(true)
    end

    reconnects = []
    servers_upon_connect = 0
    servers_after_connect = 0
    server_pool_state = nil
    with_em_timeout(10) do
      NATS.on_reconnect do |nats|
        reconnects << nats.connected_server
        server_pool_state = nats.server_pool
      end

      NATS.connect(:servers => [@s1.uri], :dont_randomize_servers => true) do |nats|
        servers_upon_connect = nats.server_pool.count
        expect(nats.server_pool.first[:uri]).to eql(nats.connected_server)

        @s3.start_server(true)
        EM.add_timer(2) do
          # Should have detected new server asynchronously
          servers_after_connect = nats.server_pool.count
          expect(nats.server_pool.first[:uri]).to eql(nats.connected_server)
          @s1.kill_server
          EM.add_timer(2) do
            @s1.start_server(true)
            @s2.kill_server
          end
        end
      end
    end
    expect(servers_upon_connect).to eql(2)
    expect(servers_after_connect).to eql(3)
    expect(reconnects.count).to eql(2)
    expect(reconnects.first.port).to eql(@s2.uri.port)
    expect(reconnects.last.port).to eql(@s3.uri.port)
    expect(server_pool_state.count).to eql(3)
    server_pool_state.each do |srv|
      expect(srv[:was_connected]).to eql(true)
    end
  end
end
