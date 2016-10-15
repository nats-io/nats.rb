require 'spec_helper'

describe 'Client - cluster reconnect' do

  before(:all) do
    auth_options = {
      'user'     => 'secret',
      'password' => 'password',
      'token'    => 'asdf',
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
          timeout: #{auth_options["timeout"]}
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

  before(:each) do
    [@s1, @s2, @s3].each do |s|
      s.start_server(true)
    end
  end

  after(:each) do
    [@s1, @s2, @s3].each do |s|
      s.kill_server
    end
  end

  it 'should connect to another server if possible before reconnect' do
    @s3.kill_server

    mon = Monitor.new
    reconnected = mon.new_cond

    nats = NATS::IO::Client.new
    nats.connect(:servers => [@s1.uri, @s2.uri], :dont_randomize_servers => true)

    disconnects = 0
    nats.on_disconnect do
      disconnects += 1
    end

    closes = 0
    nats.on_close do
      closes += 1
    end

    reconnects = 0
    nats.on_reconnect do
      reconnects += 1
      mon.synchronize do
        reconnected.signal
      end
    end

    msgs = []
    nats.subscribe("hello") do |msg|
      msgs << msg
    end
    nats.flush
    expect(nats.connected_server).to eql(@s1.uri)

    10.times do |n|
      nats.flush if n == 4
      @s1.kill_server if n == 5
      nats.publish("hello", "world.#{n}")
      sleep 0.1
    end

    mon.synchronize do
      reconnected.wait(1)
    end
    expect(nats.connected_server).to eql(@s2.uri)
    nats.close

    expect(reconnects).to eql(1)
    expect(disconnects).to eql(2)
    expect(closes).to eql(1)
  end
end
