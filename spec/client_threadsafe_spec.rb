require 'spec_helper'

describe 'Client - Thread safety' do

  before(:all) do
    @s = NatsServerControl.new
    @s.start_server(true)
  end

  after(:all) do
    @s.kill_server
  end

  class Component
    attr_reader  :nats, :options
    attr_accessor :msgs

    def initialize(options={})
      @nats = NATS::IO::Client.new
      @msgs = []
      @options = options
    end

    def connect!
      @nats.connect(@options)
    end
  end

  it 'should be able to send and receive messages from different threads' do
    component = Component.new
    component.connect!

    thr_a = Thread.new do
      component.nats.subscribe("hello") do |data, reply, subject|
        component.msgs << { :data => data, :subject => subject }
      end
      sleep 0.01 until component.msgs.count > 100
    end

    thr_b = Thread.new do
      component.nats.subscribe("world") do |data, reply, subject|
        component.msgs << { :data => data, :subject => subject }
      end
      sleep 0.01 until component.msgs.count > 100
    end

    # More than enough for subscriptions to have been flushed already.
    sleep 1

    thr_c = Thread.new do
      (1..100).step(2) do |n|
        component.nats.publish("hello", "#{n}")
      end
      component.nats.flush
    end

    thr_d = Thread.new do
      (0..99).step(2) do |n|
        component.nats.publish("world", "#{n}")
      end
      component.nats.flush
    end

    sleep 1
    expect(component.msgs.count).to eql(100)

    result = component.msgs.select { |msg| msg[:subject] != "hello" && msg[:data].to_i % 2 == 1 }
    expect(result.count).to eql(0)

    result = component.msgs.select { |msg| msg[:subject] != "world" && msg[:data].to_i % 2 == 0 }
    expect(result.count).to eql(0)

    result = component.msgs.select { |msg| msg[:subject] == "hello" && msg[:data].to_i % 2 == 1 }
    expect(result.count).to eql(50)

    result = component.msgs.select { |msg| msg[:subject] == "world" && msg[:data].to_i % 2 == 0 }
    expect(result.count).to eql(50)
  end
end
