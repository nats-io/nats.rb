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

describe 'Client - Headers' do

  before(:each) do
    @s = NatsServerControl.new("nats://127.0.0.1:4523")
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
end
