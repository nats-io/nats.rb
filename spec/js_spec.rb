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
    @s = NatsServerControl.new("nats://127.0.0.1:4524", "/tmp/test-nats.pid", "-js -sd=#{@tmpdir}")
    @s.start_server(true)
  end

  after(:each) do
    @s.kill_server
    FileUtils.remove_entry(@tmpdir)
  end

  describe 'Publish' do
    it 'should publish messages to a stream' do
      nc = NATS.connect(@s.uri)

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

  describe 'Pull Subscribe' do
    before(:each) do
      nc = NATS.connect(@s.uri)
      stream_req = {
        name: "test",
        subjects: ["test"]
      }
      resp = nc.request("$JS.API.STREAM.CREATE.test", stream_req.to_json)
      expect(resp).to_not be_nil
      nc.close
    end

    after(:each) do
      nc = NATS.connect(@s.uri)
      stream_req = {
        name: "test",
        subjects: ["test"]
      }
      resp = nc.request("$JS.API.STREAM.DELETE.test", stream_req.to_json)
      expect(resp).to_not be_nil
      nc.close
    end

    it 'should pull subscribe and fetch messages' do
      nc = NATS.connect(@s.uri)
      js = nc.jetstream

      consumer_req = {
        stream_name: "test",
        config: {
          durable_name: "test",
          ack_policy: "explicit",
          max_ack_pending: 20,
          max_waiting: 3,
          ack_wait: 5 * 1_000_000_000 # 5 seconds
        }
      }
      resp = nc.request("$JS.API.CONSUMER.DURABLE.CREATE.test.test", consumer_req.to_json)
      expect(resp).to_not be_nil

      # Send 10 messages...
      1.upto(10) { |n| js.publish("test", "hello: #{n}") }

      sub = js.pull_subscribe("test", "test", stream: "test")

      # Fetch 1, leave 9 pending.
      msgs = sub.fetch(1)
      msgs.each do |msg|
        msg.ack
      end
      msg = msgs.first
      expect(msg.data).to eql("hello: 1")

      meta = msg.metadata
      expect(meta.sequence.stream).to eql(1)
      expect(meta.sequence.consumer).to eql(1)
      expect(meta.domain).to eql("")
      expect(meta.num_delivered).to eql(1)
      expect(meta.num_pending).to eql(9)
      expect(meta.stream).to eql("test")
      expect(meta.consumer).to eql("test")

      # Check again that the parsing is memoized.
      meta = msg.metadata
      expect(meta.sequence.stream).to eql(1)
      expect(meta.sequence.consumer).to eql(1)

      # Confirm the metadata.o
      time_since = Time.now - meta.timestamp
      expect(time_since).to be_between(0, 1)

      # Confirm that cannot double ack a message.
      [:ack, :ack_sync, :nak, :term].each do |method_sym|
        expect do
          msg.send(method_sym)
        end.to raise_error(NATS::JetStream::InvalidJSAck)
      end

      # Fetch 1 more, should be 8 pending now.
      msgs = sub.fetch(1)
      msg = msgs.first
      msg.ack
      expect(msg.data).to eql("hello: 2")

      resp = nc.request("$JS.API.CONSUMER.INFO.test.test")
      info = JSON.parse(resp.data, symbolize_names: true)
      expect(info).to include({
          num_waiting: 0,
          num_ack_pending: 0,
          num_pending: 8,
        })
      expect(info[:delivered]).to include({
          consumer_seq: 2,
          stream_seq: 2
        })
      expect(info[:delivered][:consumer_seq]).to eql(2)
      expect(info[:delivered][:stream_seq]).to eql(2)

      # Fetch all the 8 pending messages.
      msgs = sub.fetch(8, timeout: 1)
      expect(msgs.count).to eql(8)

      i = 3
      msgs.each do |msg|
        expect(msg.data).to eql("hello: #{i}")
        msg.ack
        i += 1
      end

      # Pull Subscribe only works with #fetch
      expect do
        sub.next_msg
      end.to raise_error(NATS::JetStream::Error)

      # Invalid fetch sizes are errors.
      expect do
        sub.fetch(-1)
      end.to raise_error(NATS::JetStream::Error)

      expect do
        sub.fetch(0)
      end.to raise_error(NATS::JetStream::Error)

      # Nothing pending.
      resp = nc.request("$JS.API.CONSUMER.INFO.test.test")
      info = JSON.parse(resp.data, symbolize_names: true)
      expect(info).to include({
          num_waiting: 0,
          num_ack_pending: 0,
          num_pending: 0,
        })
      expect(info[:delivered]).to include({
          consumer_seq: 10,
          stream_seq: 10
        })
      expect(sub.pending_queue.size).to eql(0)

      # Publish 5 more messages.
      11.upto(15) { |n| js.publish("test", "hello: #{n}") }
      nc.flush

      resp = nc.request("$JS.API.CONSUMER.INFO.test.test")
      info = JSON.parse(resp.data, symbolize_names: true)
      expect(info).to include({
          num_waiting: 0,
          num_ack_pending: 0,
          num_pending: 5,
        })
      expect(info[:delivered]).to include({
          consumer_seq: 10,
          stream_seq: 10
        })

      # Only 5 messages will be received, with 2 pending though won't be delivered yet.
      # This should take as long as the timeout but should not throw an exception since
      # the client at least received a few messagers.
      msgs = sub.fetch(7, timeout: 2)
      expect(msgs.count).to eql(5)
      expect(sub.pending_queue.size).to eql(0)

      i = 11
      msgs.each do |msg|
        expect(msg.data).to eql("hello: #{i}")
        msg.ack
        i += 1
      end

      resp = nc.request("$JS.API.CONSUMER.INFO.test.test")
      info = JSON.parse(resp.data, symbolize_names: true)
      expect(info).to include({
          num_waiting: 0,
          num_ack_pending: 0,
          num_pending: 0,
        })
      expect(info[:delivered]).to include({
          consumer_seq: 15,
          stream_seq: 15
        })

      # 10 more messages
      16.upto(25) { |n| js.publish("test", "hello: #{n}") }
      nc.flush

      # No new messages delivered yet...
      sleep 0.5
      expect(sub.pending_queue.size).to eql(0)

      resp = nc.request("$JS.API.CONSUMER.INFO.test.test")
      info = JSON.parse(resp.data, symbolize_names: true)
      expect(info).to include({
          num_waiting: 0,
          num_ack_pending: 0,
          num_pending: 10,
        })
      expect(info[:delivered]).to include({
          consumer_seq: 15,
          stream_seq: 15
        })

      # Get 10 messages which are the total (25).
      msgs = sub.fetch(10)
      i = 16
      msgs.each do |msg|
        expect(msg.data).to eql("hello: #{i}")
        msg.ack
        i += 1
      end
      expect(msgs.count).to eql(10)
      nc.flush

      # Should have not been no more messages!
      expect do
        sub.fetch(1, timeout: 1)
      end.to raise_error(NATS::IO::Timeout)

      resp = nc.request("$JS.API.CONSUMER.INFO.test.test")
      info = JSON.parse(resp.data, symbolize_names: true)
      expect(info).to include({
          num_waiting: 1,
          num_ack_pending: 0,
          num_pending: 0,
        })
      expect(info[:delivered]).to include({
          consumer_seq: 25,
          stream_seq: 25
        })
      expect(sub.pending_queue.size).to eql(0)
      expect(i).to eql(26)

      # There should be no more messages.
      expect do
        msgs = sub.fetch(10, timeout: 1)
        expect(msgs.count).to eql(0)
      end.to raise_error(NATS::IO::Timeout)
      expect(sub.pending_queue.size).to eql(0)

      # Requests that have timed out so far will linger.
      resp = nc.request("$JS.API.CONSUMER.INFO.test.test")
      info = JSON.parse(resp.data, symbolize_names: true)
      expect(info).to include({
          num_waiting: 2,
          num_ack_pending: 0,
          num_pending: 0,
        })
      expect(info[:delivered]).to include({
          consumer_seq: 25,
          stream_seq: 25
        })

      # Make a lot of requests to get a request timeout error.
      ts = []
      errors = []
      3.times do
        ts << Thread.new do
          begin
            sub.fetch(2, timeout: 5)
          rescue => e
            errors << e
          end
        end
      end
      ts.each {|t| t.join }

      expect(errors.count > 0).to eql(true)
      e = errors.first
      expect(e).to be_a(NATS::IO::Timeout).and having_attributes(message: "nats: fetch timeout")

      resp = nc.request("$JS.API.CONSUMER.INFO.test.test")
      info = JSON.parse(resp.data, symbolize_names: true)
      expect(info[:num_waiting]).to be_between(1, 3)

      # This should not cause 408 timeout errors.
      10.times do
        expect do
          sub.fetch(1, timeout: 0.5)
        end.to raise_error(NATS::IO::Timeout)
        resp = nc.request("$JS.API.CONSUMER.INFO.test.test")
        info = JSON.parse(resp.data, symbolize_names: true)
        expect(info[:num_waiting]).to be_between(1, 3)
      end

      # Force request timeout errors.
      ts = []
      5.times do
        ts << Thread.new do
          begin
            msgs = sub.fetch(1, timeout: 0.5)
            expect(msgs).to be_empty
          rescue => e
            errors << e
          end
        end
      end
      ts.each do |t|
        t.join
      end
      api_err = errors.select { |o| o.is_a?(NATS::JetStream::APIError) }
      expect(api_err).to_not be_empty
      expect(api_err.first.code).to eql("408")

      nc.close
    end
  end
end
