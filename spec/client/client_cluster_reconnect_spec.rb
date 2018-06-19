require 'spec_helper'

describe 'Client - cluster reconnect' do

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

  it 'should properly handle exceptions thrown by eventmachine during reconnects' do
    reconnect_cb = false
    opts = {
      :dont_randomize_servers => true,
      :reconnect_time_wait => 0.25,
      :servers => [@s1.uri, URI.parse("nats://does.not.exist:4222/"), @s3.uri]
    }
    with_em_timeout(5) do
      nc = NATS.connect(opts)
      nc.on_reconnect do
        reconnect_cb = true
        expect(nc.connected?).to be(true)
        expect(nc.connected_server).to eql(@s3.uri)
      end

      EM.add_timer(1) do
        @s1.kill_server
      end
    end
    expect(reconnect_cb).to eql(true)
  end

  it 'should call reconnect callback when current connection fails' do
    reconnect_cb = false
    opts = {
      :dont_randomize_servers => true,
      :reconnect_time_wait => 0.25,
      :servers => [@s1.uri, @s2.uri, @s3.uri],
      :max_reconnect_attempts => 5
    }
    reconnect_conns = []
    with_em_timeout(5) do
      NATS.start(opts) do |c|
        expect(c.connected_server).to eql(@s1.uri)
        c.on_reconnect do |conn|
          reconnect_cb = true
          reconnect_conns << conn.connected_server
        end
        @s1.kill_server
        EM.add_timer(1) do
          @s2.kill_server
        end
      end
    end

    expect(reconnect_cb).to eq(true)
    expect(reconnect_conns.count).to eql(2)
    expect(reconnect_conns.first).to eql(@s2.uri)
    expect(reconnect_conns.last).to eql(@s3.uri)
  end

  it 'should connect to another server if possible before reconnect' do
    NATS.start(:dont_randomize_servers => true, :servers => [@s1.uri, @s2.uri, @s3.uri]) do |c|
      timeout_nats_on_failure(15)
      expect(c.connected_server).to eql(@s1.uri)
      c.on_reconnect do
        expect(c.connected_server).to eql(@s2.uri)
        NATS.stop
      end
      @s1.kill_server
    end
  end

  it 'should connect to a previous server if multiple servers exit' do
    NATS.start(:dont_randomize_servers => true, :servers => [@s1.uri, @s2.uri, @s3.uri]) do |c|
      timeout_nats_on_failure(15)
      expect(c.connected_server).to eql(@s1.uri)
      kill_time = Time.now

      expected_uri = nil
      c.on_reconnect do
        expect(c.connected_server).to eql(expected_uri)
        if expected_uri == @s2.uri
          # Expect to connect back to S1
          expected_uri = @s1.uri
          @s1.start_server
          @s2.kill_server
        end
        NATS.stop if c.connected_server == @s1.uri
      end

      # Expect to connect to S2 after killing S1 and S3.
      expected_uri = @s2.uri

      @s1.kill_server
      @s3.kill_server
    end
  end

  it 'should use reconnect logic to connect to a previous server if multiple servers exit' do
    @s2.kill_server # Take this one offline
    @s3.kill_server # Take this one offline too, since it will be discovered in cluster

    options = {
      :dont_randomize_servers => true,
      :servers => [@s1.uri, @s2.uri],
      :reconnect => true,
      :max_reconnect_attempts => 2,
      :reconnect_time_wait => 1
    }
    with_em_timeout do
      NATS.start(options) do |c|
        expect(c.connected_server).to eql(@s1.uri)
        c.on_reconnect do
          expect(c.connected?).to eql(true)
          expect(c.connected_server).to eql(@s1.uri)
          NATS.stop
        end
        @s1.kill_server
        @s1.start_server
      end
    end
  end

  context 'when max_reconnect_attempts == -1 (do not remove servers)' do
    it 'should never remove servers that fail' do
      options = {
        :dont_randomize_servers => true,
        :servers => [@s1.uri, @s2.uri],
        :reconnect => true,
        :max_reconnect_attempts => -1,
        :reconnect_time_wait => 1
      }

      done = false
      @s1.kill_server
      with_em_timeout(2) do
        NATS.start(options) do |c|
          expect(c.connected_server).to eql(@s2.uri)
          expect(c.server_pool.size).to eq(3)
          EM.add_timer(0.5) do
            @s3.kill_server # Server goes away from the cluster
            EM.add_timer(1) do
              done = true
              expect(c.server_pool.size).to eq(3)
            end
          end
        end
      end
      expect(done).to eql(true)
    end

    it 'should never remove servers due to client triggered disconnect' do

      options = {
        :dont_randomize_servers => true,
        :servers => [@s1.uri, @s2.uri],
        :reconnect => true,
        :max_reconnect_attempts => -1,
        :reconnect_time_wait => 2
      }

      done = false
      with_em_timeout(5) do |future|
        errors = []
        reconnects = 0
        NATS.start(options) do |nc|
          nc.on_error do |e|
            errors << e
          end

          nc.on_reconnect do
            reconnects += 1
          end
          expect(nc.connected_server).to eql(@s1.uri)
          expect(nc.server_pool.size).to eq(3)

          messages = []
          nc.subscribe("hello") do |msg|
            messages << msg
          end

          payload = nc.server_info[:max_payload] + 100
          nc.publish("hello", 'A' * payload)

          EM.add_timer(1) { @s1.kill_server }
          EM.add_timer(2.5) do
            expect(nc.connected_server).to eql(@s2.uri)

            expect(nc.server_pool.size).to eq(3)
            expect(messages.count).to eql(0)

            EM.add_timer(0.5) do
              @s3.kill_server
              expect(errors.count).to eql(1)
              expect(nc.server_pool.size).to eq(3)
              done = true
              nc.close
              future.resume
            end
          end
        end
      end
      expect(done).to eql(true)
    end
  end
end
