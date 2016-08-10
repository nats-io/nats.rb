require 'spec_helper'

describe 'Client - cluster' do

  before(:all) do
    auth_options = {
      'user'     => 'derek',
      'password' => 'bella',
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
          timeout: #{auth_options["timeout"]}
        }

        cluster {
          host: '#{config_opts['host']}'
          port: #{config_opts['cluster_port']}

          authorization {
            user: foo
            password: bar
            timeout: #{auth_options["timeout"]}
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
      s.start_server(true) unless NATS.server_running? s.uri
    end
  end

  after(:each) do
    [@s1, @s2].each do |s|
      s.kill_server
    end
  end

  it 'should properly route plain messages between different servers' do
    data = 'Hello World!'
    received = 0
    with_em_timeout do
      c1 = NATS.connect(:uri => @s1.uri) do |nats|
        nats.subscribe('foo') do |msg|
          expect(msg).to eql(data)
          received += 1
        end
      end
      c2 = NATS.connect(:uri => @s2.uri) do |nats|
        nats.subscribe('foo') do |msg|
          expect(msg).to eql(data)
          received += 1
        end
      end

      c1.flush do
        c2.flush do
          wait_on_routes_connected([c1, c2]) do
            c2.publish('foo', data)
            c2.publish('foo', data)
            flush_routes([c1, c2]) { EM.stop }
          end
        end
      end
    end
    expect(received).to eql(4)
  end

  it 'should properly route messages with staggered startup' do

    @s2.kill_server
    data = 'Hello World!'
    received = 0
    EM.run do
      c1 = NATS.connect(:uri => @s1.uri) do
        c1.subscribe('foo') do |msg|
          expect(msg).to eql(data)
          received += 1
        end
        c1.flush do
          @s2.start_server
          c2 = NATS.connect(:uri => @s2.uri) do
            flush_routes([c1, c2]) do
              c2.publish('foo', data)
              c2.publish('foo', data)
              flush_routes([c1, c2]) { EM.stop }
            end
          end
        end
      end
    end
    expect(received).to eql(2)
  end
end
