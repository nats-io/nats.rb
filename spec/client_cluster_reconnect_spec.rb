require 'spec_helper'

describe 'Client - Cluster reconnect' do

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
            'nats-route://foo:bar@127.0.0.1:#{s1_config_opts['cluster_port']}'
          ]
        }
      ), config_opts)
    end

    @s1, @s2, @s3 = nodes
  end

  context 'with cluster fully assembled when client connects' do
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

    it 'should gracefully reconnect to another available server while publishing' do
      @s3.kill_server

      mon = Monitor.new
      reconnected = mon.new_cond

      nats = NATS::IO::Client.new
      nats.connect({
                     servers: [@s1.uri, @s2.uri],
                     dont_randomize_servers: true
                   })

      disconnects = 0
      nats.on_disconnect do |e|
        disconnects += 1
      end

      closes = 0
      nats.on_close do
        closes += 1
      end

      reconnects = 0
      nats.on_reconnect do |s|
        reconnects += 1
        mon.synchronize do
          reconnected.signal
        end
      end

      errors = []
      nats.on_error do |e|
        errors << e
      end

      msgs = []
      nats.subscribe("hello.*") do |msg|
        msgs << msg
      end
      nats.flush
      expect(nats.connected_server).to eql(@s1.uri)

      msg_payload = "A" * 10_000
      1000.times do |n|
        # Receive 100 messages initially and then failover
        case
        when n == 100
          nats.flush
          expect(msgs.count).to eql(100)
          @s1.kill_server
        when (n % 100 == 0)
          # yield a millisecond
          sleep 0.001
        end

        # Messages sent here can be lost
        nats.publish("hello.#{n}", msg_payload)
      end

      # Flush everything we have sent so far
      nats.flush(5)
      errors = []
      errors.each do |e|
        errors << e
      end
      mon.synchronize { reconnected.wait(1) }
      expect(nats.connected_server).to eql(@s2.uri)
      nats.close

      expect(reconnects).to eql(1)
      expect(disconnects).to eql(2)
      expect(closes).to eql(1)
      expect(errors).to be_empty
    end
  end

  context 'with auto discovery using seed node' do
    before(:each) do
      # Only start initial seed node
      @s1.start_server(true)
    end

    after(:each) do
      @s1.kill_server
    end

    it 'should reconnect to nodes discovered from seed server' do
      # Nodes join to cluster before we try to connect
      [@s2, @s3].each do |s|
        s.start_server(true)
      end

      begin
        mon = Monitor.new
        reconnected = mon.new_cond

        nats = NATS::IO::Client.new
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

        # Connect to first server only and trigger reconnect
        nats.connect(:servers => [@s1.uri], :dont_randomize_servers => true)
        expect(nats.connected_server).to eql(@s1.uri)
        @s1.kill_server
        sleep 0.1
        mon.synchronize do
          reconnected.wait(3)
        end
        expect(nats.connected_server).to eql(@s2.uri)
        expect(reconnects).to eql(1)
        expect(disconnects).to eql(1)
        expect(closes).to eql(0)

        nats.close
      ensure
        # Wrap up test
        [@s2, @s3].each do |s|
          s.kill_server
        end
      end
    end

    it 'should reconnect to nodes discovered in the cluster after first connect' do
      mon = Monitor.new
      reconnected = mon.new_cond

      nats = NATS::IO::Client.new
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

      # Connect to first server only and trigger reconnect
      nats.connect(:servers => [@s1.uri], :dont_randomize_servers => true, :user => 'secret', :pass => 'password')
      expect(nats.connected_server).to eql(@s1.uri)

      begin
        # Couple of servers join...
        [@s2, @s3].each do |s|
          s.start_server(true)
        end
        nats.flush

        # Wait for a bit before disconnecting from original server
        nats.flush
        @s1.kill_server
        mon.synchronize do
          reconnected.wait(3)
        end

        # We still consider the original node and we have new ones
        # which can be used to failover.
        expect(nats.servers.count).to eql(3)
        expect(nats.discovered_servers.count).to eql(2)
        expect(nats.connected_server).to eql(@s2.uri)
        expect(reconnects).to eql(2)
        expect(disconnects).to eql(2)
        expect(closes).to eql(0)

        nats.close
      ensure
        # Wrap up test
        [@s2, @s3].each do |s|
          s.kill_server
        end
      end
    end
  end
end
