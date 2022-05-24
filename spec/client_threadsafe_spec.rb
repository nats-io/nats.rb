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
    nc = NATS.connect(servers: ['nats://0.0.0.0:4222'])

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

  it 'should connect once across threads' do
    nc = NATS.connect(@s.uri)
    nc.subscribe(">") { }
    nc.subscribe("help") do |msg, reply|
      nc.publish(reply, "OK!")
    end

    nc2 = NATS::IO::Client.new
    cids = []
    ts = []
    responses = []
    ts << Thread.new do
      nc2.connect(@s.uri)

      loop do
        begin
          nc2.publish("foo", 'bar', 'quux')
        rescue => e
          break
        end
        sleep 0.01
      end
    end

    5.times do
      ts << Thread.new do
        # connect should be idempotent across threads.
        nc.connect(@s.uri)
        si = nc.instance_variable_get("@server_info")
        cids << si[:client_id]
        responses << nc.request("help", "hi")
        nc.flush
      end
    end
    sleep 2

    ts.each do |t|
      t.exit
    end
    nc.close
    nc2.close

    results = cids.uniq
    expect(results.count).to eql(1)

    expect(responses.count).to eql(5)
  end

  # Using pure-nats.rb in a Ractor requires URI 0.11.0 or greater due to URI Ractor support.
  major_version, minor_version, _ = Gem.loaded_specs['uri'].version.to_s.split('.').map(&:to_i)
  if major_version >= 0 && minor_version >= 11
    it 'should be able to process messages in a Ractor' do
      nc = NATS.connect(@s.uri)

      messages = []
      nc.subscribe('foo') do |msg|
        messages << msg
      end

      r1 = Ractor.new(@s.uri) do |uri|
        r_nc = NATS.connect(uri)

        r_nc.publish('foo', 'bar')
        r_nc.flush
        r_nc.close

        'r1 Finished'
      end
      r1.take # wait for Ractor to finish sending messages
      expect(messages.count).to eql(1)

      r2 = Ractor.new(@s.uri) do |uri|
        r_nc = NATS.connect(uri)

        r_messages = []
        r_nc.subscribe('bar') do |payload, reply|
          r_nc.publish(reply, 'OK!')
          r_messages << payload
        end

        Ractor.yield 'r2 Ready'
        sleep 0.01 while r_messages.empty?
      end
      r2.take # wait for Ractor to finish setup

      response = nil
      expect do
        response = nc.request('bar', 'baz', timeout: 0.5)
      end.to_not raise_error

      expect(response.data).to eql('OK!')
    end
  end
end
