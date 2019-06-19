# Copyright 2016-2018 The NATS Authors
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

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

    it 'should connect to another server if possible before reconnect using multiple uris' do
      @s3.kill_server

      mon = Monitor.new
      reconnected = mon.new_cond

      nats = NATS::IO::Client.new
      nats.connect("nats://secret:password@127.0.0.1:4242,nats://secret:password@127.0.0.1:4243", :dont_randomize_servers => true)

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
      expect(nats.connected_server.to_s).to eql(@s1.uri.to_s)

      10.times do |n|
        nats.flush if n == 4
        @s1.kill_server if n == 5
        nats.publish("hello", "world.#{n}")
        sleep 0.1
      end

      mon.synchronize do
        reconnected.wait(1)
      end
      expect(nats.connected_server.to_s).to eql(@s2.uri.to_s)
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
      expect(nats.connected_server.to_s).to eql(@s1.uri.to_s)

      msg_payload = "A" * 10_000
      1000.times do |n|
        # Receive 100 messages initially and then failover
        case
        when n == 100
          nats.flush

          # Wait a bit for all messages
          sleep 0.5
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

        errors = []
        nats.on_error do |e|
          errors << e
        end

        # Connect to first server only and trigger reconnect
        nats.connect(:servers => [@s1.uri], :dont_randomize_servers => true, :reconnect => true)
        expect(nats.connected_server).to eql(@s1.uri)
        @s1.kill_server
        sleep 0.1
        mon.synchronize do
          reconnected.wait(3)
        end

        # Reconnected...
        expect(nats.connected_server).to eql(@s2.uri)
        expect(reconnects).to eql(1)
        expect(disconnects).to eql(1)
        expect(closes).to eql(0)
        expect(errors.count).to eql(1)
        expect(errors.first).to be_a(Errno::ECONNRESET)

        # There should be no error since we reconnected now
        expect(nats.last_error).to eql(nil)

        nats.close
      ensure
        # Wrap up test
        [@s2, @s3].each do |s|
          s.kill_server
        end
      end
    end

    it 'should reconnect to nodes discovered from seed server with single uri' do
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

        errors = []
        nats.on_error do |e|
          errors << e
        end

        # Connect to first server only and trigger reconnect
        nats.connect("nats://secret:password@127.0.0.1:4242", :dont_randomize_servers => true, :reconnect => true)
        expect(nats.connected_server.to_s).to eql(@s1.uri.to_s)
        @s1.kill_server
        sleep 0.1
        mon.synchronize do
          reconnected.wait(3)
        end

        # Reconnected...
        expect(nats.connected_server).to eql(@s2.uri)
        expect(reconnects).to eql(1)
        expect(disconnects).to eql(1)
        expect(closes).to eql(0)
        expect(errors.count).to eql(1)
        expect(errors.first).to be_a(Errno::ECONNRESET)

        # There should be no error since we reconnected now
        expect(nats.last_error).to eql(nil)

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

      errors = []
      nats.on_error do |e|
        errors << e
      end

      # Connect to first server only and trigger reconnect
      nats.connect({
        :servers => [@s1.uri],
        :dont_randomize_servers => true,
        :user => 'secret',
        :pass => 'password',
        :max_reconnect_attempts => 10
      })
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

        # Only 2 new ones should be discovered servers even after reconnect
        expect(nats.discovered_servers.count).to eql(2)
        expect(nats.connected_server).to eql(@s2.uri)
        expect(reconnects).to eql(1)
        expect(disconnects).to eql(1)
        expect(closes).to eql(0)
        expect(errors.count).to eql(2)
        expect(errors.first).to be_a(Errno::ECONNRESET)
        expect(errors.last).to be_a(Errno::ECONNREFUSED)
        expect(nats.last_error).to be_a(Errno::ECONNREFUSED)

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
