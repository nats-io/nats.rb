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
require 'tmpdir'

describe 'JetStream' do
  before(:each) do
    @tmpdir = Dir.mktmpdir("ruby-jetstream")
    @s = NatsServerControl.new("nats://127.0.0.1:4523", "/tmp/test-nats.pid", "-js -sd=#{@tmpdir}")
    @s.start_server(true)
  end

  after(:each) do
    @s.kill_server
    FileUtils.remove_entry(@tmpdir)
  end

  describe 'Publish' do
    it 'should publish messages to a stream' do
      nc = NATS::IO::Client.new
      nc.connect(servers: [@s.uri])

      # Create sample Stream and pull based consumer from JetStream
      # from which it will be attempted to fetch messages using no_wait.
      stream_req = {
        name: "foojs",
        subjects: ["foo.js"]
      }

      # Create the stream.
      resp = nc.request("$JS.API.STREAM.CREATE.foojs", stream_req.to_json)
      expect(resp).to_not be_nil

      # Get JetStream context.
      js = nc.jetstream

      1.upto(100) do |n|
        ack = js.publish("foo.js", "hello world")
        expect(ack[:stream]).to eql("foojs")
        expect(ack[:seq]).to eql(n)
      end

      # Assert stream name.
      expect do
        ack = js.publish("foo.js", "hello world", stream: "bar")
      end.to raise_error(NATS::JetStream::APIError)

      begin
        js.publish("foo.js", "hello world", stream: "bar")
      rescue NATS::JetStream::APIError => e
        expect(e.code).to eql(400)
      end

      expect do
        js.publish("foo.bar", "hello world")
      end.to raise_error(NATS::JetStream::NoStreamResponseError)

      nc.close
    end
  end
end
