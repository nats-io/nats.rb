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

    nodes = []
    configs = [s1_config_opts, s2_config_opts, s3_config_opts]
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

    @s1, @s2, @s3 = nodes
  end

  after(:each) do
    [@s1, @s2, @s3].each do |s|
      s.kill_server if NATS.server_running?(s.uri)
    end
  end

  it 'should properly discover nodes in cluster upon connect' do
    # Start servers and form a cluster, client will only be aware of first node
    # though it will discover the other nodes in the cluster automatically.
    [@s1, @s2, @s3].each do |node|
      node.start_server(true)
    end

    servers_upon_connect = 0
    c1_msgs = []
    with_em_timeout do
      c1 = NATS.connect(:servers => [@s1.uri]) do |nats|
        servers_upon_connect = nats.server_pool.count
        NATS.stop
      end

      c1.subscribe("hello") do |msg|
        c1_msgs << msg
      end
    end
    expect(servers_upon_connect).to eql(3)
  end

  it 'should properly discover nodes in cluster eventually after first connect' do
    [@s1, @s2].each do |node|
      node.start_server(true)
    end

    servers_upon_connect = 0
    servers_after_connect = 0
    with_em_timeout(5) do
      c1 = NATS.connect(:servers => [@s2.uri]) do |nats|
        servers_upon_connect = nats.server_pool.count

        @s3.start_server(true)
        EM.add_timer(2) do
          # Should have detected new server asynchronously
          servers_after_connect = nats.server_pool.count
        end
      end
    end
    expect(servers_upon_connect).to eql(2)
    expect(servers_after_connect).to eql(3)
  end  
end
