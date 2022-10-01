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
  describe 'Publish' do
    before(:each) do
      @tmpdir = Dir.mktmpdir("ruby-jetstream")
      @s = NatsServerControl.new("nats://127.0.0.1:4524", "/tmp/test-nats.pid", "-js -sd=#{@tmpdir}")
      @s.start_server(true)
    end

    after(:each) do
      @s.kill_server
      FileUtils.remove_entry(@tmpdir)
    end

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
        expect(ack.stream).to eql("foojs")
        expect(ack.seq).to eql(n)
      end

      # Assert stream name.
      expect do
        ack = js.publish("foo.js", "hello world", stream: "bar")
      end.to raise_error(NATS::JetStream::API::Error)

      begin
        js.publish("foo.js", "hello world", stream: "bar")
      rescue NATS::JetStream::API::Error => e
        expect(e.code).to eql(400)
      end

      expect do
        js.publish("foo.bar", "hello world")
      end.to raise_error(NATS::JetStream::Error::NoStreamResponse)

      nc.close
    end
  end

  describe 'Pull Subscribe' do
    before(:each) do
      @tmpdir = Dir.mktmpdir("ruby-jetstream")
      @s = NatsServerControl.new("nats://127.0.0.1:4524", "/tmp/test-nats.pid", "-js -sd=#{@tmpdir}")
      @s.start_server(true)
    end

    after(:each) do
      @s.kill_server
      FileUtils.remove_entry(@tmpdir)
    end

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

    let(:nc) { NATS.connect(@s.uri) }

    it "should auto create pull subscription" do
      js = nc.jetstream
      js.add_stream(name: "hello", subjects: ["hello", "world", "hello.>"])

      js.publish("hello", "1")
      js.publish("world", "2")
      js.publish("hello.world", "3")

      sub = js.pull_subscribe("hello", "psub", config: { max_waiting: 30 })
      info = sub.consumer_info
      expect(info.config.max_waiting).to eql(30)
      expect(info.num_pending).to eql(3)

      msgs = sub.fetch(3)
      msgs.each do |msg|
        msg.ack
      end

      # Using the API type.
      # TODO: Handle mismatch between config and current state.
      config = NATS::JetStream::API::ConsumerConfig.new(max_waiting: 128)
      sub = js.pull_subscribe("hello", "psub2", config: config)
      info = sub.consumer_info
      expect(info.config.max_waiting).to eql(128)
      expect(info.num_pending).to eql(3)

      msgs = sub.fetch(1)
      msgs.each do |msg|
        msg.ack_sync
      end
      info = sub.consumer_info
      expect(info.num_pending).to eql(2)
    end

    it 'should find the pull subscription by subject' do
      nc = NATS.connect(@s.uri)
      js = nc.jetstream

      consumer_req = {
        stream_name: "test",
        config: {
          durable_name: "test-find",
          ack_policy: "explicit",
          max_ack_pending: 20,
          max_waiting: 3,
          ack_wait: 5 * 1_000_000_000 # 5 seconds
        }
      }
      resp = nc.request("$JS.API.CONSUMER.DURABLE.CREATE.test.test-find", consumer_req.to_json)
      expect(resp).to_not be_nil

      sub = js.pull_subscribe("test", "test-find")
      expect(sub.sid).to eql(2)
      sub.unsubscribe
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
        end.to raise_error(NATS::JetStream::Error::MsgAlreadyAckd)
      end

      # This one is ok to ack multiple times.
      msg.in_progress

      # Fetch 1 more, should be 8 pending now.
      msgs = sub.fetch(1)
      msg = msgs.first
      msg.ack
      expect(msg.data).to eql("hello: 2")
      sleep 0.5

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
          num_waiting: 0,
          num_ack_pending: 0,
          num_pending: 0,
        })
      expect(info[:delivered]).to include({
          consumer_seq: 25,
          stream_seq: 25
        })
      # expect(sub.pending_queue.size).to eql(0)
      expect(i).to eql(26)

      # There should be no more messages.
      expect do
        msgs = sub.fetch(10, timeout: 1)
        expect(msgs.count).to eql(0)
      end.to raise_error(NATS::IO::Timeout)
      # expect(sub.pending_queue.size).to eql(1)

      # Requests that have timed out so far will linger.
      resp = nc.request("$JS.API.CONSUMER.INFO.test.test")
      info = JSON.parse(resp.data, symbolize_names: true)
      expect(info).to include({
          num_waiting: 0,
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
            msgs = sub.fetch(2, timeout: 0.2)
          rescue => e
            errors << e
          end
        end
      end
      ts.each {|t| t.join }

      expect(errors.count > 0).to eql(true)
      e = errors.first
      expect(e).to be_a(NATS::IO::Timeout)

      # NOTE: After +2.7.1 info also resets the expired requests.
      #
      # resp = nc.request("$JS.API.CONSUMER.INFO.test.test")
      # info = JSON.parse(resp.data, symbolize_names: true)
      # expect(info[:num_waiting]).to be_between(1, 3)
      #

      # This should not cause 408 timeout errors.
      10.times do
        expect do
          sub.fetch(1, timeout: 0.5)
        end.to raise_error(NATS::IO::Timeout)
        # resp = nc.request("$JS.API.CONSUMER.INFO.test.test")
        # info = JSON.parse(resp.data, symbolize_names: true)
        # expect(info[:num_waiting]).to be_between(1, 3)
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
      api_err = errors.select { |o| o.is_a?(NATS::Timeout) }
      expect(api_err).to_not be_empty
      # expect(api_err.first.code).to eql("408")

      nc.close
    end

    it 'should unsubscribe' do
      nc = NATS.connect(@s.uri)
      js = nc.jetstream

      consumer_req = {
        stream_name: "test",
        config: {
          durable_name: "delsub",
          ack_policy: "explicit",
          max_ack_pending: 20,
          max_waiting: 3,
          ack_wait: 5 * 1_000_000_000 # 5 seconds
        }
      }
      resp = nc.request("$JS.API.CONSUMER.DURABLE.CREATE.test.delsub", consumer_req.to_json)
      expect(resp).to_not be_nil

      sub = js.pull_subscribe("test", "delsub", stream: "test")

      expect do
        msgs = sub.fetch(1, timeout: 0.5)
      end.to raise_error(NATS::IO::Timeout)

      ack = js.publish("test", "hello")
      expect(ack.seq).to eql(1)
      msgs = sub.fetch
      expect(msgs.size).to eql(1)

      ack = js.publish("test", "hello")
      expect(ack.seq).to eql(2)

      sub.unsubscribe

      expect do
        msgs = sub.fetch(1, timeout: 0.5)
      end.to raise_error(NATS::Timeout)

      expect do
        sub.unsubscribe
      end.to raise_error(NATS::IO::BadSubscription)
    end

    it 'should account pending data' do
      nc = NATS.connect(@s.uri)
      nc2 = NATS.connect(@s.uri)
      js = nc.jetstream
      subject = "limits.test"

      nc.on_error do |e|
        puts e
      end

      js.add_stream(name: "limitstest", subjects: [subject])

      # Continuously send messages until reaching pending bytes limit.
      t = Thread.new do
        payload = 'A' * 1024 * 1024
        loop do
          nc2.publish(subject, payload)
          sleep 0.01
        end
      end

      sub = js.pull_subscribe(subject, "test")
      65.times do |i|
        msgs = sub.fetch(1)
        msgs.each do |msg|
          msg.ack
        end
      end

      sub = js.pull_subscribe(subject, "test")
      65.times do |i|
        msgs = sub.fetch(2)
        msgs.each do |msg|
          msg.ack
        end
      end

      t.exit
      nc.close
      nc2.close
    end
  end

  describe 'Push Subscribe' do
    before(:each) do
      @tmpdir = Dir.mktmpdir("ruby-jetstream")
      @s = NatsServerControl.new("nats://127.0.0.1:4527", "/tmp/test-nats.pid", "-js -sd=#{@tmpdir}")
      @s.start_server(true)
    end

    after(:each) do
      @s.kill_server
      FileUtils.remove_entry(@tmpdir)
    end

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

    let(:nc) { NATS.connect(@s.uri) }

    it "should create ephemeral subscription with auto and manual ack" do
      js = nc.jetstream
      js.add_stream(name: "hello", subjects: ["hello", "world", "hello.>"])

      js.publish("hello", "1")
      js.publish("world", "2")
      js.publish("hello.world", "3")
      js.publish("hello", "2")

      # Auto Ack
      future = Future.new
      msgs = []
      sub = js.subscribe("hello.world") do |msg|
        # They will be auto acked
        msgs << msg
        future.set_result(msgs)
      end
      msgs = future.wait_for(1)
      expect(msgs.count).to eql(1)

      sleep 0.5
      info = sub.consumer_info
      expect(info.stream_name).to eql("hello")
      expect(info.num_pending).to eql(0)
      expect(info.num_ack_pending).to eql(0)

      # Attempting to ack again is an error.
      expect do
        msgs[0].ack
      end.to raise_error(NATS::JetStream::Error::MsgAlreadyAckd)

      # Manual Ack
      future = Future.new
      msgs = []
      sub = js.subscribe("hello", manual_ack: true) do |msg|
        msgs << msg
        future.set_result(msgs) if msgs.count == 2
      end
      msgs = future.wait_for(1)
      expect(msgs.count).to eql(2)
      expect(msgs[0].data).to eql('1')
      expect(msgs[1].data).to eql('2')

      info = sub.consumer_info
      expect(info.stream_name).to eql("hello")
      expect(info.num_pending).to eql(0)
      expect(info.num_ack_pending).to eql(2)
      msgs.each { |msg| msg.ack_sync }
      sleep 1

      info = sub.consumer_info
      expect(info.num_ack_pending).to eql(0)
      sub.unsubscribe

      # Without callback
      sub = js.subscribe("hello")
      msg = sub.next_msg
      msg.ack_sync
      info = sub.consumer_info
      expect(info.stream_name).to eql("hello")
      expect(info.num_pending).to eql(0)
      expect(info.num_ack_pending).to eql(1)
      sub.unsubscribe
    end

    it "should create durable single subscribers" do
      js = nc.jetstream
      js.add_stream(name: "hello", subjects: ["hello", "world", "hello.>"])

      js.publish("hello", "1")
      js.publish("world", "2")
      js.publish("hello.world", "3")
      js.publish("hello", "2")

      # Cannot have queue and durable be different.
      expect do
        js.subscribe("hello", queue: "foo", durable: "hello")
      end.to raise_error(NATS::JetStream::Error)

      # Async susbcriber
      future = Future.new
      msgs = []
      sub = js.subscribe("hello", durable: "first", manual_ack: true) do |msg|
        msgs << msg
        future.set_result(msgs) if msgs.count == 2
      end
      msgs = future.wait_for(1)
      expect(msgs.count).to eql(2)

      # Resubscribing should fail since already push bound
      expect do
        js.subscribe("hello", durable: "first")
      end.to raise_error(NATS::JetStream::Error)

      info = sub.consumer_info
      expect(info.num_ack_pending).to eql(2)
      sub.unsubscribe

      # Trigger a redelivery of the messages.
      msgs.each do |msg|
        msg.nak
      end

      info = sub.consumer_info
      expect(info.num_pending).to eql(0)
      expect(info.num_ack_pending).to eql(2)

      # Sync subscribe to get the same messages again.
      sub = js.subscribe("hello", durable: "first")
      msg = sub.next_msg
      msg.ack

      info = sub.consumer_info
      expect(info.num_pending).to eql(0)
      expect(info.num_ack_pending).to eql(1)
    end

    it "should create subscriber with a queue" do
      js = nc.jetstream

      # Cannot have queue and durable be different.
      qsubs = []
      5.times do
        qsub = js.subscribe("test", queue: "foo", manual_ack: true)
        qsubs << qsub
      end

      50.times do |i|
        js.publish("test", "#{i}")
      end

      # Each should get at least a couple of messages
      qsubs.each do |qsub|
        expect(qsub.pending_queue.size > 2).to eql(true)
      end
    end

    it "should create subscribers with custom config" do
      js = nc.jetstream
      js.add_stream(name:"custom", subjects:["custom"])

      1.upto(10).each do |i|
        js.publish("custom", "n:#{i}")
      end

      sub = js.subscribe("custom", durable: 'example', config: { deliver_policy: 'new' })

      js.publish("custom", "last")
      msg = sub.next_msg

      expect(msg.data).to eql("last")
      expect(msg.metadata.sequence.stream).to_not eql(1)
      expect(msg.metadata.sequence.consumer).to eql(1)

      cinfo = js.consumer_info("custom", "example")
      expect(cinfo.config[:deliver_policy]).to eql("new")

      nc.close
    end
  end

  describe 'Domain' do
    before(:each) do
      @tmpdir = Dir.mktmpdir("ruby-jetstream-domain")
      config_opts = {
        'pid_file'      => '/tmp/nats_js_domain_1.pid',
        'host'          => '127.0.0.1',
        'port'          => 4729,
      }
      @domain = "estre"
      @s = NatsServerControl.init_with_config_from_string(%Q(
        port = #{config_opts['port']}
        jetstream {
          domain = #{@domain}
          store_dir = "#{@tmpdir}"
        }
      ), config_opts)
      @s.start_server(true)
    end

    after(:each) do
      @s.kill_server
      FileUtils.remove_entry(@tmpdir)
    end

    it 'should produce, consume and ack messages in a stream' do
      nc = NATS.connect(@s.uri)

      # Create stream in the domain.
      subject = "foo"
      stream_name = "test"
      stream_req = {
        name: stream_name,
        subjects: [subject]
      }
      resp = nc.request("$JS.#{@domain}.API.STREAM.CREATE.#{stream_name}",
                        stream_req.to_json)
      expect(resp).to_not be_nil

      # Now create a consumer in the domain.
      durable_name = "test"
      consumer_req = {
        stream_name: stream_name,
        config: {
          durable_name: "test",
          ack_policy: "explicit",
          max_ack_pending: 20,
          max_waiting: 3,
          ack_wait: 5 * 1_000_000_000 # 5 seconds
        }
      }
      resp = nc.request("$JS.#{@domain}.API.CONSUMER.DURABLE.CREATE.#{stream_name}.#{durable_name}",
                        consumer_req.to_json)
      expect(resp).to_not be_nil

      # Create producer with custom domain.
      producer = nc.JetStream(domain: @domain)
      ack = producer.publish(subject)
      expect(ack[:stream]).to eql(stream_name)
      expect(ack[:domain]).to eql(@domain)
      expect(ack[:seq]).to eql(1)

      # Without domain would work as well in this case.
      js = nc.JetStream()
      ack = js.publish(subject)
      expect(ack[:stream]).to eql(stream_name)
      expect(ack[:domain]).to eql(@domain)
      expect(ack[:seq]).to eql(2)

      # Connecting to wrong domain should fail.
      js = nc.JetStream(domain: "stok")
      expect do
        js.pull_subscribe(subject, durable_name, stream: stream_name)
      end.to raise_error(NATS::JetStream::Error::ServiceUnavailable)

      # Check pending acks before fetching.
      resp = nc.request("$JS.#{@domain}.API.CONSUMER.INFO.#{stream_name}.#{durable_name}")
      info = JSON.parse(resp.data, symbolize_names: true)
      expect(info[:num_pending]).to eql(2)

      js = nc.JetStream(domain: @domain)
      sub = js.pull_subscribe(subject, durable_name, stream: stream_name)
      msgs = sub.fetch(1)
      msg = msgs.first
      msg.ack_sync

      # Confirm ack went through.
      resp = nc.request("$JS.#{@domain}.API.CONSUMER.INFO.#{stream_name}.#{durable_name}")
      info = JSON.parse(resp.data, symbolize_names: true)
      expect(info[:num_pending]).to eql(1)

      js = nc.jetstream(domain: "estre")
      info = js.account_info
      expected = {
        :type => "io.nats.jetstream.api.v1.account_info_response",
        :memory => 0,
        :storage => 66,
        :streams => 1,
        :consumers => 1,
        :limits => {
          :max_memory => -1,
          :max_storage => -1,
          :max_streams => -1,
          :max_consumers => -1,
          :max_ack_pending => -1,
          :memory_max_stream_bytes => -1,
          :storage_max_stream_bytes => -1,
          :max_bytes_required => false
        },
        :domain => "estre",
        :api => {:total => 5, :errors => 0}
      }
      expect(expected).to eql(info)
    end

    it 'should bail when stream or consumer does not exist in domain' do
      nc = NATS.connect(@s.uri)
      js = nc.JetStream(domain: @domain)

      # Should try to auto lookup and fail.
      expect do
        js.pull_subscribe("foo", "bar")
      end.to raise_error(NATS::JetStream::Error::NotFound)

      # Invalid stream name.
      expect do
        js.pull_subscribe("foo", "bar", stream: "")
      end.to raise_error(NATS::JetStream::Error::InvalidStreamName)

      # Stream that does not exist.
      expect do
        sub = js.pull_subscribe("foo", "bar", stream: "nonexistent")
      end.to raise_error(NATS::JetStream::Error::StreamNotFound)

      # Now create the stream.
      stream_req = {
        name: "foo",
        subjects: ["foo"]
      }
      resp = nc.request("$JS.#{@domain}.API.STREAM.CREATE.foo", stream_req.to_json)
      expect(resp).to_not be_nil

      # Should find the stream now but fail to find the consumer.
      expect do
        js.pull_subscribe("foo", "bar", stream: "foo")
      end.to raise_error(NATS::JetStream::Error::ConsumerNotFound)

      consumer_req = {
        stream_name: "foo",
        config: {
          durable_name: "test-find",
          ack_policy: "explicit",
          max_ack_pending: 20,
          max_waiting: 3,
          ack_wait: 5 * 1_000_000_000 # 5 seconds
        }
      }
      resp = nc.request("$JS.API.CONSUMER.DURABLE.CREATE.foo.test-find", consumer_req.to_json)
      expect(resp).to_not be_nil

      # FIXME: This should return a not found error instead of empty response.
      # {:type=>"io.nats.jetstream.api.v1.stream_names_response", :total=>0, :offset=>0, :limit=>1024, :streams=>nil}
      # sub = js.pull_subscribe("missing", "bar")
      js.pull_subscribe("foo", "test-find")
    end
  end

  describe "Errors" do
    it "NATS::Error" do
      expect do
        raise NATS::IO::Timeout
      end.to raise_error(NATS::Error)

      expect do
        raise NATS::IO::SocketTimeoutError
      end.to raise_error(NATS::Error)

      expect do
        raise NATS::IO::SocketTimeoutError
      end.to raise_error(NATS::Timeout)
    end

    it "JetStream::Error" do
      # NATS::Error can catch either JetStream or NATS errors.
      expect do
        raise NATS::JetStream::Error
      end.to raise_error(NATS::Error)

      expect do
        raise NATS::IO::Error
      end.to raise_error(NATS::Error)

      expect do
        raise NATS::IO::Timeout
      end.to raise_error(NATS::Error)

      expect do
        raise NATS::Timeout
      end.to raise_error(NATS::Error)
    end

    it "JetStream::API::Error" do
      expect do
        raise NATS::JetStream::Error::ConsumerNotFound
      end.to raise_error(NATS::JetStream::API::Error)

      expect do
        raise NATS::JetStream::Error::ConsumerNotFound
      end.to raise_error(NATS::Error)

      expect do
        raise NATS::JetStream::Error::StreamNotFound
      end.to raise_error(NATS::JetStream::API::Error)

      expect do
        raise NATS::JetStream::Error::StreamNotFound
      end.to raise_error(NATS::Error)

      expect do
        raise NATS::JetStream::Error::ConsumerNotFound
      end.to raise_error(NATS::JetStream::Error::NotFound)

      expect do
        raise NATS::JetStream::Error::ServiceUnavailable
      end.to raise_error(NATS::JetStream::Error)

      expect do
        raise NATS::JetStream::Error::ServiceUnavailable
      end.to raise_error(NATS::JetStream::API::Error)

      expect do
        raise NATS::JetStream::Error::ServiceUnavailable
      end.to raise_error(NATS::Error)

      expect do
        raise NATS::JetStream::Error::ServiceUnavailable
      end.to raise_error(an_instance_of(NATS::JetStream::Error::ServiceUnavailable)
                           .and having_attributes(code: 503))

      expect do
        raise NATS::JetStream::Error::NotFound
      end.to raise_error(an_instance_of(NATS::JetStream::Error::NotFound)
                           .and having_attributes(code: 404))

      expect do
        raise NATS::JetStream::Error::BadRequest
      end.to raise_error(an_instance_of(NATS::JetStream::Error::BadRequest)
                           .and having_attributes(code: 400))
    end
  end

  describe 'JSM' do
    before(:each) do
      @tmpdir = Dir.mktmpdir("ruby-jsm")
      config_opts = {
        'pid_file'      => '/tmp/nats_jsm_1.pid',
        'host'          => '127.0.0.1',
        'port'          => 4730,
      }
      @domain = "estre"
      @s = NatsServerControl.init_with_config_from_string(%Q(
        port = #{config_opts['port']}
        jetstream {
          domain = #{@domain}
          store_dir = "#{@tmpdir}"
        }
      ), config_opts)
      @s.start_server(true)
    end

    after(:each) do
      @s.kill_server
      FileUtils.remove_entry(@tmpdir)
    end

    let(:nc) { NATS.connect(@s.uri) }

    it "should support jsm.add_stream" do
      stream_config = {
        name: "mystream"
      }
      resp = nc.jsm.add_stream(stream_config)
      expect(resp).to be_a NATS::JetStream::API::StreamCreateResponse
      expect(resp.type).to eql('io.nats.jetstream.api.v1.stream_create_response')
      expect(resp.config.name).to eql('mystream')
      expect(resp.config.num_replicas).to eql(1)

      resp = nc.jsm.add_stream(name: 'stream2')
      expect(resp).to be_a NATS::JetStream::API::StreamCreateResponse
      expect(resp.config.name).to eql('stream2')
      expect(resp.config.num_replicas).to eql(1)

      # Can also use the types for the request.
      stream_config = {
        name: "stream3"
      }
      config = NATS::JetStream::API::StreamConfig.new(stream_config)
      resp = nc.jsm.add_stream(config)
      expect(resp).to be_a NATS::JetStream::API::StreamCreateResponse
      expect(resp.config.name).to eql('stream3')
      expect(resp.config.num_replicas).to eql(1)

      expect do
        nc.jsm.add_stream(foo: "foo")
      end.to raise_error(ArgumentError)

      expect do
        nc.jsm.add_stream(foo: "foo.*")
      end.to raise_error(ArgumentError)

      # Raise when stream names contain prohibited characters
      expect do
        nc.jsm.add_stream(name: "foo.bar*baz")
      end.to raise_error(ArgumentError)

      placement = { cluster: "foo", tags: ["a"]}
      resp = nc.jsm.add_stream(name: "v29",
                               subjects: ["v29"],
                               num_replicas: 1,
                               no_ack: true,
                               # allow_direct: true,
                               placement: placement
                               )
      expect(resp).to be_a NATS::JetStream::API::StreamCreateResponse
      # expect(resp.config.allow_direct).to eql(true)
      expect(resp.config.no_ack).to eql(true)
      expect(resp.config.placement).to eql(placement)

      nc.close
    end

    it "should support jsm.stream_info" do
      nc.jsm.add_stream(name: "a")
      info = nc.jsm.stream_info("a")
      expect(info).to be_a NATS::JetStream::API::StreamInfo
      expect(info.state).to be_a NATS::JetStream::API::StreamState
      expect(info.config).to be_a NATS::JetStream::API::StreamConfig
      expect(info.created).to be_a Time
      nc.close
    end

    it "should support jsm.delete_stream" do
      stream_name = "stream-to-delete"
      info = nc.jsm.add_stream(name: stream_name)
      stream_info = nc.jsm.stream_info(stream_name)
      expect(info.config).to eql(stream_info.config)
      ok = nc.jsm.delete_stream(stream_name)
      expect(ok).to eql(true)

      expect do
        nc.jsm.stream_info(stream_name)
      end.to raise_error NATS::JetStream::Error::NotFound
    end

    it "should support jsm.add_consumer" do
      stream_name = "add-consumer-test"
      nc.jsm.add_stream(name: stream_name, subjects: ["foo"])

      # Create durable consumer
      consumer_config = {
        durable_name: "test-create",
        ack_policy: "explicit",
        max_ack_pending: 20,
        max_waiting: 3,
        ack_wait: 5
      }
      resp = nc.jsm.add_consumer(stream_name, consumer_config)
      expect(resp).to be_a NATS::JetStream::API::ConsumerInfo
      expect(resp.stream_name).to eql(stream_name)
      expect(resp.name).to eql("test-create")

      # Create ephemeral consumer (with deliver subject).
      inbox = nc.new_inbox
      consumer_config = {
        deliver_subject: inbox,
        ack_policy: "explicit",
        max_ack_pending: 20,
        ack_wait: 5 # seconds
      }
      consumer = nc.jsm.add_consumer(stream_name, consumer_config)
      expect(consumer).to be_a NATS::JetStream::API::ConsumerInfo
      expect(consumer.stream_name).to eql(stream_name)
      expect(consumer.name).to_not eql("")
      expect(consumer.config.deliver_subject).to eql(inbox)

      js = nc.jetstream
      js.publish("foo", "Hello World!")

      # Now lookup the consumer using the ephemeral name.
      info = nc.jsm.consumer_info(stream_name, consumer.name)

      # Fetch with pull subscribe.
      psub = js.pull_subscribe("foo", "test-create")
      msgs = psub.fetch
      expect(msgs.count).to eql(1)
      msgs.each do |msg|
        msg.ack_sync
      end

      # Fetch with ephemeral.
      sub = nc.subscribe(inbox)
      msg = sub.next_msg(timeout: 1)
      resp = msg.ack_sync
      expect(resp).to_not be_nil

      expect do
        sub.next_msg(timeout: 0.5)
      end.to raise_error NATS::Timeout

      # Create durable consumer
      consumer_config = {
        durable_name: "test-create2",
        num_replicas: 3
      }
      # It should fail to set replicas since not enough nodes.
      # expect do
      #   nc.jsm.add_consumer(stream_name, consumer_config)
      # end.to raise_error NATS::JetStream::Error::ServerError
    end

    it "should support jsm.delete_consumer" do
      stream_name = "to-delete"
      consumer_name = "dur"
      nc.jsm.add_stream(name: stream_name)
      nc.jsm.add_consumer(stream_name, { durable_name: consumer_name})
      info = nc.jsm.consumer_info(stream_name, consumer_name)
      ok = nc.jsm.delete_consumer(stream_name, consumer_name)
      expect(ok).to eql(true)
    end

    it "should support jsm.find_stream_name_by_subject" do
      stream_req = {
        name: "foo",
        subjects: ["a", "a.*"]
      }
      resp = nc.request("$JS.API.STREAM.CREATE.foo", stream_req.to_json)
      expect(resp).to_not be_nil

      stream_req = {
        name: "bar",
        subjects: ["b", "b.*"]
      }
      resp = nc.request("$JS.API.STREAM.CREATE.bar", stream_req.to_json)
      expect(resp).to_not be_nil

      js = nc.jetstream
      stream = js.find_stream_name_by_subject("a")
      expect(stream).to eql("foo")

      stream = js.find_stream_name_by_subject("a.*")
      expect(stream).to eql("foo")

      stream = js.find_stream_name_by_subject("b")
      expect(stream).to eql("bar")

      stream = js.find_stream_name_by_subject("b.*")
      expect(stream).to eql("bar")

      expect do
        js.find_stream_name_by_subject("c")
      end.to raise_error(NATS::JetStream::Error::NotFound)

      expect do
        js.find_stream_name_by_subject("c")
      end.to raise_error(NATS::JetStream::API::Error)

      expect do
        js.find_stream_name_by_subject("c", timeout: 0.00001)
      end.to raise_error(NATS::Timeout)

      nc.close
    end

    it "should support jsm.consumer_info" do
      nc = NATS.connect(@s.uri)

      stream_req = {
        name: "quux",
        subjects: ["q"]
      }
      resp = nc.request("$JS.API.STREAM.CREATE.quux", stream_req.to_json)
      expect(resp).to_not be_nil

      consumer_req = {
        stream_name: "quux",
        config: {
          durable_name: "test",
          ack_policy: "explicit",
          max_ack_pending: 20,
          max_waiting: 3,
          ack_wait: 5 * 1_000_000_000 # 5 seconds
        }
      }
      resp = nc.request("$JS.API.CONSUMER.DURABLE.CREATE.quux.test", consumer_req.to_json)
      expect(resp).to_not be_nil

      js = nc.jetstream

      js.publish("q", "hello world")

      info = js.consumer_info("quux", "test")
      expect(info.type).to eql("io.nats.jetstream.api.v1.consumer_info_response")

      # It is a struct so either is ok.
      expect(info.num_pending).to eql(1)
      expect(info[:num_pending]).to eql(1)
      expect(info.stream_name).to eql("quux")
      expect(info.name).to eql("test")

      # Cannot modify the response.
      expect do
        info.num_pending = 10
      end.to raise_error(FrozenError)

      expect do
        js.consumer_info("quux", "missing")
      end.to raise_error(NATS::JetStream::API::Error)

      expect do
        js.consumer_info("quux", "missing")
      end.to raise_error(NATS::JetStream::Error::NotFound)
    end
  end
end
