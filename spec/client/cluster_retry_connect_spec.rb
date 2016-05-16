require 'spec_helper'

describe 'Client - cluster retry connect' do

  before(:all) do
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

  it 'should re-establish asymmetric route connections upon restart' do
    data = 'Hello World!'
    received = 0
    with_em_timeout(5) do |future|
      c1 = NATS.connect(:uri => @s1.uri)
      c2 = NATS.connect(:uri => @s2.uri)

      c1.subscribe('foo') do |msg|
        expect(msg).to eql(data)
        received += 1

        if received == 2
          future.resume # proper exit
        elsif received == 1
          # Here we will kill s1, which does not actively connect to anyone.
          # Upon restart we will make sure the route was re-established properly.

          @s1.kill_server
          @s1.start_server

          wait_on_routes_connected([c1, c2]) do
            expect(c1.connected_server).to eql(@s1.uri)
            c2.publish('foo', data)
          end
        end
      end

      wait_on_routes_connected([c1, c2]) do
        c2.publish('foo', data)
      end

    end

    expect(received).to eql(2)
  end

end
