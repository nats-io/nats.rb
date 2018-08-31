require 'spec_helper'
require 'yaml'

describe 'Client - auth token' do

  before(:all) do

    auth_options = {
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
          token: '#{auth_options["token"]}'
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
      s.start_server(true)
    end
  end

  after(:each) do
    [@s1, @s2].each do |s|
      s.kill_server
    end
  end

  let(:auth_token) {
    'deadbeef'
  }

  it 'should properly connect to different servers using token' do
    EM.run do
      c1 = NATS.connect(:uri => @s1.uri, :token => auth_token)
      c2 = NATS.connect(:uri => "nats://#{auth_token}@#{@s1.uri.host}:#{@s1.uri.port}")
      c3 = NATS.connect("nats://#{auth_token}@#{@s1.uri.host}:#{@s1.uri.port}")
      wait_on_connections([c1, c2, c3]) do
        EM.stop
      end
    end
  end

  it 'should raise auth error when using wrong token' do
    expect do
      with_em_timeout do
        NATS.connect(:uri => @s1.uri, :token => 'wrong', :allow_reconnect => false)
      end
    end.to raise_error(NATS::AuthError)

    expect do
      with_em_timeout do
        NATS.connect(:uri => "nats://wrong@#{@s1.uri.host}:#{@s1.uri.port}")
      end
    end.to raise_error(NATS::AuthError)

    expect do
      with_em_timeout do
        NATS.connect("nats://wrong@#{@s1.uri.host}:#{@s1.uri.port}")
      end
    end.to raise_error(NATS::AuthError)
  end

  it 'should reuse token for reconnecting' do
    data = 'Hello World!'
    to_send = 100
    received = c1_received = c2_received = 0
    reconnected = false
    with_em_timeout(3) do
      c1 = NATS.connect(:uri => @s1.uri, :token => auth_token)
      c2 = NATS.connect(:uri => @s2.uri, :token => auth_token)
      c1.on_reconnect do
        reconnected = true
      end

      c1.subscribe('foo', :queue => 'bar') do |msg|
        expect(msg).to eql(data)
        received += 1
      end
      c2.subscribe('foo', :queue => 'bar') do |msg|
        expect(msg).to eql(data)
        received += 1
      end

      wait_on_routes_connected([c1, c2]) do
        (1..to_send).each { c2.publish('foo', data) }
      end

      EM.add_timer(0.5) do
        @s1.kill_server
        EM.add_timer(1) do
          (1..to_send).each { c2.publish('foo', data) }
        end
      end
    end

    expect(received).to eql(to_send*2)
    expect(reconnected).to eql(true)
  end
end
