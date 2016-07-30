require 'spec_helper'
require 'uri'

describe 'Client - cluster load balance' do

  before(:each) do
    auth_options = {
      'user'     => 'derek',
      'password' => 'bella',
      'token'    => 'deadbeef',
      'timeout'  => 1
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

    nodes = []
    configs = [s1_config_opts, s2_config_opts]
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
          timeout: 0.5
        }

        cluster {
          host: '#{config_opts['host']}'
          port: #{config_opts['cluster_port']}

          authorization {
            user: foo
            password: bar
            timeout: 1
          }

          routes = [
            #{routes.join("\n            ")}
          ]
        }
      ), config_opts)
    end

    @s1, @s2 = nodes
  end

  before(:each) do
    [@s1, @s2].each do |s|
      s.start_server(true)
    end
  end

  after(:each) do
    [@s1, @s2].each do |s|
      s.kill_server
    end
  end

  # Tests that the randomize pool works correctly and that not
  # all clients are connecting to the same server.
  it 'should properly load balance between multiple servers with same client config' do
    clients = []
    servers = { @s1.uri => 0, @s2.uri => 0 }
    EM.run do
      for i in 0...20
        clients << NATS.connect(:uri => servers.keys)
      end
      results = {}
      wait_on_connections(clients) do
        clients.each do |c|
          servers[c.connected_server] += 1
          port, ip = Socket.unpack_sockaddr_in(c.get_peername)
          expect(port).to eql(URI(c.connected_server).port)
        end

        servers.each_value do |v|
          expect(v > 0).to eql(true)
        end
        EM.stop
      end
    end
  end
end
