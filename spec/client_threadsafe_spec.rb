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

    threads = []
    thr_a = Thread.new do
      component.nats.subscribe("hello") do |data, reply, subject|
        component.msgs << { :data => data, :subject => subject }
      end
      sleep 0.01 until component.msgs.count > 100
    end
    threads << thr_a

    thr_b = Thread.new do
      component.nats.subscribe("world") do |data, reply, subject|
        component.msgs << { :data => data, :subject => subject }
      end
      sleep 0.01 until component.msgs.count > 100
    end
    threads << thr_b

    # More than enough for subscriptions to have been flushed already.
    sleep 1

    thr_c = Thread.new do
      (1..100).step(2) do |n|
        component.nats.publish("hello", "#{n}")
      end
      component.nats.flush
    end
    threads << thr_c

    thr_d = Thread.new do
      (0..99).step(2) do |n|
        component.nats.publish("world", "#{n}")
      end
      component.nats.flush
    end
    threads << thr_d

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

    component.nats.close

    threads.each do |t|
      t.kill
    end
  end

  it 'should allow async subscriptions to process messages in parallel' do
    nc = NATS::IO::Client.new
    nc.connect(servers: ['nats://0.0.0.0:4222'])

    foo_msgs = []
    nc.subscribe('foo') do |payload|
      foo_msgs << payload
      sleep 1
    end

    bar_msgs = []
    nc.subscribe('bar') do |payload, reply|
      bar_msgs << payload
      nc.publish(reply, 'OK!')
    end

    quux_msgs = []
    nc.subscribe('quux') do |payload, reply|
      quux_msgs << payload
    end

    # Receive on message foo first which takes longer to process.
    nc.publish('foo', 'hello')

    # Publish many messages to quux which should be able to consume fast.
    1.upto(10).each do |n|
      nc.publish('quux', "test-#{n}")      
    end

    # Awaiting for the response happens on the same
    # thread where the request is happening, then
    # the read loop thread is going to signal back.
    response = nil
    expect do
      response = nc.request('bar', 'help', timeout: 0.5)
    end.to_not raise_error

    expect(response.data).to eql('OK!')

    # Wait a bit in case all of this happened too fast
    sleep 0.2
    expect(foo_msgs.count).to eql(1)
    expect(bar_msgs.count).to eql(1)
    expect(quux_msgs.count).to eql(10)

    1.upto(10).each do |n|
      expect(quux_msgs[n-1]).to eql("test-#{n}")
    end
    nc.close
  end
end
