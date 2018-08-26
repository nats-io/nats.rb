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
end
