require 'spec_helper'

describe 'Client - Connect' do

  before(:each) do
    @s = NatsServerControl.new
    @s.start_server(true)
  end

  after(:each) do
    @s.kill_server
  end

  context "No echo support" do
    it "should not receive messages when no echo is enabled" do
      errors = []
      msgs_a = []
      msgs_b = []
      with_em_timeout do
        NATS.on_error do |e|
          p e
          errors << e
        end

        # Client A will receive all messages from B
        NATS.connect(uri: @s.uri, no_echo: true) do |nc|
          nc.subscribe("hello") do |msg|
            msgs_a << msg
          end

          EM.add_timer(0.5) do
            10.times do
              nc.publish("hello", "world")
            end
          end
        end

        # Default is classic behavior of client which is
        # for echo to be enabled, so will receive messages
        # from Client A and itself.
        NATS.connect(uri: @s.uri) do |nc|
          nc.subscribe("hello") do |msg|
            msgs_b << msg
          end

          EM.add_timer(0.5) do
            10.times do
              nc.publish("hello", "world")
            end
          end
        end
      end

      expect(errors.count).to eql(0)
      expect(msgs_a.count).to eql(10)
      expect(msgs_b.count).to eql(20)
    end

    it "should have no echo enabled by default" do
      errors = []
      msgs_a = []
      msgs_b = []
      with_em_timeout do
        NATS.on_error do |e|
          errors << e
        end

        # Client A will receive all messages from B
        NATS.connect(uri: @s.uri) do |nc|
          nc.subscribe("hello") do |msg|
            msgs_a << msg
          end

          EM.add_timer(0.5) do
            10.times do
              nc.publish("hello", "world")
            end
          end
        end

        # Default is classic behavior of client which is
        # for echo to be enabled, so will receive messages
        # from Client A and itself.
        NATS.connect(uri: @s.uri) do |nc|
          nc.subscribe("hello") do |msg|
            msgs_b << msg
          end

          EM.add_timer(0.5) do
            10.times do
              nc.publish("hello", "world")
            end
          end
        end
      end

      expect(errors.count).to eql(0)
      expect(msgs_a.count).to eql(20)
      expect(msgs_b.count).to eql(20)
    end

    it "should fail if echo is enabled but not supported by server" do
      expect do
        with_em_timeout do
          OldInfoServer.start {
            NATS.connect(uri: "nats://127.0.0.1:9997", no_echo: true) do |nc|
            end
          }
        end
      end.to raise_error(NATS::ServerError)
    end

    it "should fail if echo is enabled but not supported by server protocol" do
      expect do
        with_em_timeout do
          OldProtocolInfoServer.start {
            NATS.connect(uri: "nats://127.0.0.1:9996", no_echo: true) do |nc|
            end
          }
        end
      end.to raise_error(NATS::ServerError)
    end
  end

  context "Simple Connect" do
    it "should connect using host:port" do
      with_em_timeout do |future|
        NATS.connect("127.0.0.1:4222") do |nc|
          nc.subscribe("foo") do
            future.resume
          end
          nc.publish("foo", "bar")
        end
      end
    end

    it "should connect with just host using default port" do
      with_em_timeout do |future|
        NATS.connect("127.0.0.1") do |nc|
          nc.subscribe("foo") do
            future.resume
          end
          nc.publish("foo", "bar")
        end
      end
    end

    it "should connect with just host: using default port" do
      with_em_timeout do |future|
        NATS.connect("127.0.0.1:") do |nc|
          nc.subscribe("foo") do
            future.resume
          end
          nc.publish("foo", "bar")
        end
      end
    end

    it "should fail to connect with empty string" do
      expect do
        with_em_timeout do |future|
          NATS.connect("")
        end
      end.to raise_error(URI::InvalidURIError)
    end

    it "should support comma separated list of servers" do
      options = {}
      servers = []
      with_em_timeout do |future|
        NATS.connect("nats://127.0.0.1:4222,nats://127.0.0.1:4223,nats://127.0.0.1:4224", dont_randomize_servers: true) do |nc|
          nc.subscribe("foo") do
            options = nc.options
            servers = nc.server_pool
            future.resume
          end
          nc.publish("foo", "bar")
        end
      end
      expect(options[:dont_randomize_servers]).to eql(true)
      expect(servers.count).to eql(3)
      servers.each do |server|        
        expect(server[:uri].scheme).to eql('nats')
        expect(server[:uri].host).to eql('127.0.0.1')
      end
      a, b, c = servers
      expect(a[:uri].port).to eql(4223)
      expect(b[:uri].port).to eql(4224)
      expect(c[:uri].port).to eql(4222)
    end

    it "should support comma separated list of servers with own user info" do
      servers = []
      with_em_timeout do |future|
        NATS.connect("nats://a:b@127.0.0.1:4222,nats://c:d@127.0.0.1:4223,nats://e:f@127.0.0.1:4224", dont_randomize_servers: true) do |nc|
          nc.subscribe("foo") do
            servers = nc.server_pool
            future.resume
          end
          nc.publish("foo", "bar")
        end
      end
      expect(servers.count).to eql(3)
      servers.each do |server|        
        expect(server[:uri].scheme).to eql('nats')
        expect(server[:uri].host).to eql('127.0.0.1')
      end
      a, b, c = servers
      expect(c[:uri].port).to eql(4222)
      expect(a[:uri].port).to eql(4223)
      expect(b[:uri].port).to eql(4224)

      expect(c[:uri].user).to eql('a')
      expect(a[:uri].user).to eql('c')
      expect(b[:uri].user).to eql('e')

      expect(c[:uri].password).to eql('b')
      expect(a[:uri].password).to eql('d')
      expect(b[:uri].password).to eql('f')
    end
  end
end
