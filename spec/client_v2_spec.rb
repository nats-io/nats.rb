# Copyright 2016-2021 The NATS Authors
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
require 'monitor'

describe 'Client - v2.2 features' do

  before(:each) do
    @s = NatsServerControl.new("nats://127.0.0.1:4523", "/tmp/test-nats.pid", "-js")
    @s.start_server(true)
  end

  after(:each) do
    @s.kill_server
    sleep 1
  end

  it 'should received a message with headers' do
    mon = Monitor.new
    done = mon.new_cond

    nc = NATS::IO::Client.new
    nc.connect(:servers => [@s.uri])

    msgs = []
    nc.subscribe("hello") do |data, _, _, header|
      msg = NATS::Msg.new(data: data, header: header)
      msgs << msg

      if msgs.count >= 5
        mon.synchronize do
          done.signal
        end
      end
    end
    nc.flush

    1.upto(5) do |n|
      data = "hello world-#{'A' * n}"
      msg = NATS::Msg.new(subject: 'hello',
                          data: data,
                          header: {
                            'foo': 'bar',
                            'hello': "hello-#{n}"
                          })
      nc.publish_msg(msg)
      nc.flush
    end

    mon.synchronize { done.wait(1) }

    expect(msgs.count).to eql(5)

    msgs.each_with_index do |msg, i|
      n = i + 1
      expect(msg.data).to eql("hello world-#{'A' * n}")
      expect(msg.header).to eql({"foo"=>"bar", "hello"=>"hello-#{n}"})
    end
    nc.close
  end

  it 'should make requests with headers' do
    nc = NATS::IO::Client.new
    nc.connect(:servers => [@s.uri])
    nc.on_error do |e|
      puts "Error: #{e}"
      puts e.backtrace
    end

    msgs = []
    seq = 0
    nc.subscribe("hello") do |data, reply, _, header|
      seq += 1
      header['response'] = seq
      msg = NATS::Msg.new(data: data, subject: reply, header: header)
      msgs << msg

      nc.publish_msg(msg)
    end
    nc.flush

    1.upto(5) do |n|
      data = "hello world-#{'A' * n}"
      msg = NATS::Msg.new(subject: 'hello',
                          data: data,
                          header: {
                            'foo': 'bar',
                            'hello': "hello-#{n}"
                          })
      resp = nc.request_msg(msg, timeout: 1)
      expect(resp.data).to eql("hello world-#{'A' * n}")
      expect(resp.header).to eql({"foo" => "bar", "hello" => "hello-#{n}", "response" => "#{n}"})
      nc.flush
    end
    expect(msgs.count).to eql(5)

    nc.close
  end

  it 'should raise no responders error by default' do
    nc = NATS.connect(servers: [@s.uri])

    expect do
      resp = nc.request("hi", "timeout", timeout: 1)
      expect(resp).to be_nil
    end.to raise_error(NATS::IO::NoRespondersError)

    expect do
      resp = nc.request("hi", "timeout", timeout: 1, old_style: true)
      expect(resp).to be_nil
    end.to raise_error(NATS::IO::NoRespondersError)

    expect do
      resp = nc.old_request("hi", "timeout", timeout: 1)
      expect(resp).to be_nil
    end.to raise_error(NATS::IO::NoRespondersError)

    resp = nil
    expect do
      msg = NATS::Msg.new(subject: "hi")
      resp = nc.request_msg(msg, timeout: 1)
    end.to raise_error(NATS::IO::NoRespondersError)

    result = nc.instance_variable_get(:@resp_map)
    expect(result.keys.count).to eql(0)

    expect(resp).to be_nil

    nc.close
  end

  it 'should not raise no responders error if no responders disabled' do
    nc = NATS::IO::Client.new
    nc.connect(servers: [@s.uri], no_responders: false)

    resp = nil
    expect do
      resp = nc.request("hi", "timeout")
    end.to raise_error(NATS::IO::Timeout)

    expect(resp).to be_nil

    # Timed out requests should be cleaned up.
    50.times do
      nc.request("hi", "timeout", timeout: 0.001) rescue nil
    end

    msg = NATS::Msg.new(subject: "hi")
    50.times do
      nc.request_msg(msg, timeout: 0.001) rescue nil
    end

    result = nc.instance_variable_get(:@resp_map)
    expect(result.keys.count).to eql(0)

    resp = nil
    expect do
      msg = NATS::Msg.new(subject: "hi")
      resp = nc.request_msg(msg)
    end.to raise_error(NATS::IO::Timeout)

    expect(resp).to be_nil

    nc.close
  end

  it 'should not raise no responders error if no responders disabled' do
    nc = NATS::IO::Client.new
    nc.connect(servers: [@s.uri], no_responders: false)

    resp = nil
    expect do
      resp = nc.request("hi", "timeout")
    end.to raise_error(NATS::IO::Timeout)

    expect(resp).to be_nil

    resp = nil
    expect do
      msg = NATS::Msg.new(subject: "hi")
      resp = nc.request_msg(msg)
    end.to raise_error(NATS::IO::Timeout)

    expect(resp).to be_nil

    nc.close
  end

  it 'should handle responses with status and description headers' do
    nc = NATS::IO::Client.new
    nc.connect(servers: [@s.uri], no_responders: true)

    # Create sample Stream and pull based consumer from JetStream
    # from which it will be attempted to fetch messages using no_wait.
    stream_req = {
      name: "foojs",
      subjects: ["foo.js"]
    }

    # Create the stream.
    resp = nc.request("$JS.API.STREAM.CREATE.foojs", stream_req.to_json)
    expect(resp).to_not be_nil 

    # Publish with ack.
    resp = nc.request("foo.js", "hello world")
    expect(resp).to_not be_nil
    expect(resp.header).to be_nil

    # Create the consumer.
    consumer_req = {
      stream_name: "foojs",
      config: {
        durable_name: "sample",
        deliver_policy: "all",
        ack_policy: "explicit",
        max_deliver: -1,
        replay_policy: "instant"
      }
    }
    resp = nc.request("$JS.API.CONSUMER.DURABLE.CREATE.foojs.sample", consumer_req.to_json)
    expect(resp).to_not be_nil

    # Get single message.
    pull_req = { no_wait: true, batch: 1}
    resp = nc.request("$JS.API.CONSUMER.MSG.NEXT.foojs.sample", pull_req.to_json, old_style: true)
    expect(resp).to_not be_nil
    expect(resp.data).to eql("hello world")

    # Fail to get next message.
    resp = nc.request("$JS.API.CONSUMER.MSG.NEXT.foojs.sample", pull_req.to_json, old_style: true)
    expect(resp).to_not be_nil
    expect(resp.header).to_not be_nil
    expect(resp.header).to eql({"Status"=>"404", "Description"=>"No Messages"})
    
    nc.close
  end
end
