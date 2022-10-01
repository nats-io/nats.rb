require 'spec_helper'
require 'monitor'
require 'tmpdir'

describe 'KeyValue' do
  before(:each) do
    @tmpdir = Dir.mktmpdir("ruby-jetstream")
    @s = NatsServerControl.new("nats://127.0.0.1:4621", "/tmp/test-nats.pid", "-js -sd=#{@tmpdir}")
    @s.start_server(true)
  end

  after(:each) do
    @s.kill_server
    FileUtils.remove_entry(@tmpdir)
  end

  it 'should support access to KeyValue stores' do
    nc = NATS.connect(@s.uri)

    js = nc.jetstream
    kv = js.create_key_value(bucket: "TEST", history: 5, ttl: 3600)
    status = kv.status
    expect(status.bucket).to eql("TEST")
    expect(status.values).to eql(0)
    expect(status.history).to eql(5)
    expect(status.ttl).to eql(3600)

    revision = kv.put("hello", "world")
    expect(revision).to eql(1)

    entry = kv.get("hello")
    expect(entry.revision).to eql(1)
    expect(entry.value).to eql("world")

    status = kv.status
    expect(status.bucket).to eql("TEST")
    expect(status.values).to eql(1)

    100.times do |i|
      kv.put("hello.#{i}", "Hello JS KV! #{i}")
    end

    status = kv.status
    expect(status.bucket).to eql("TEST")
    expect(status.values).to eql(101)

    entry = kv.get("hello.99")
    expect(entry.revision).to eql(101)
    expect(entry.value).to eql("Hello JS KV! 99")

    kv.delete("hello")

    expect do
      kv.get("hello")
    end.to raise_error(NATS::KeyValue::KeyNotFoundError)

    js.delete_key_value("TEST")

    expect do
      js.key_value("TEST")
    end.to raise_error(NATS::KeyValue::BucketNotFoundError)
  end

  it "should report when bucket is not found or invalid" do
    nc = NATS.connect(@s.uri)

    js = nc.jetstream
    expect do
      js.key_value("FOO")
    end.to raise_error(NATS::KeyValue::BucketNotFoundError)

    js.add_stream(name: "KV_foo")
    js.publish("KV_foo", "bar")

    sub = js.subscribe("KV_foo")
    msg = sub.next_msg
    msg.ack

    cinfo = sub.consumer_info
    expect(cinfo.num_pending).to eql(0)

    expect do
      js.key_value("foo")
    end.to raise_error(NATS::KeyValue::BadBucketError)
  end

  it 'should support access to KeyValue stores from multiple instances' do
    nc = NATS.connect(@s.uri)

    js = nc.jetstream
    kv = js.create_key_value(bucket: "TEST2")
    ('a'..'z').each do |l|
      kv.put(l, l*10)
    end

    nc2 = NATS.connect(@s.uri)
    js2 = nc2.jetstream
    kv2 = js2.key_value("TEST2")
    a = kv2.get("a")
    expect(a.value).to eql('aaaaaaaaaa')

    nc.close
    nc2.close
  end

  it 'should support get by revision' do
    nc = NATS.connect(@s.uri)
    js = nc.jetstream
    kv = js.create_key_value(bucket: "TEST", history: 5, ttl: 3600, description: "Basic KV")

    si = js.stream_info("KV_TEST")
    config = NATS::JetStream::API::StreamConfig.new(
        name: "KV_TEST",
        description: "Basic KV",
        subjects: ["$KV.TEST.>"],
        allow_rollup_hdrs: true,
        deny_delete: true,
        deny_purge: false,
        discard: "new",
        duplicate_window: 120 * ::NATS::NANOSECONDS,
        max_age: 3600 * ::NATS::NANOSECONDS,
        max_bytes: -1,
        max_consumers: -1,
        max_msg_size: -1,
        max_msgs: -1,
        max_msgs_per_subject: 5,
        mirror: nil,
        no_ack: nil,
        num_replicas: 1,
        placement: nil,
        retention: "limits",
        sealed: false,
        sources: nil,
        storage: "file",
        republish: nil,
        allow_direct: false,
        mirror_direct: false,
    )
    expect(config).to eql(si.config)

    # Nothing from start
    expect do
      kv.get("name")
    end.to raise_error(NATS::KeyValue::KeyNotFoundError)

    # Simple put
    revision = kv.put("name", 'alice')
    expect(revision).to eql(1)

    # Simple get
    result = kv.get("name")
    expect(result.revision).to eq(1)
    expect(result.value).to eql('alice')

    # Delete
    ok = kv.delete("name")
    expect(ok)

    # Deleting then getting again should be a not found error still,
    # although internally this is a KeyDeletedError.
    expect do
      kv.get("name")
    end.to raise_error(NATS::KeyValue::KeyNotFoundError)

    # Recreate with different name.
    revision = kv.create("name", 'bob')
    expect(revision).to eql(3)

    # Expect last revision to be 4.
    expect do
      kv.delete('name', last: 4)
    end.to raise_error(NATS::JetStream::Error::BadRequest)

     # Correct revision should work.
    revision = kv.delete("name", last: 3)
    expect(revision).to eql(4)

    # Conditional Updates.
    revision = kv.update("name", "hoge", last: 4)
    expect(revision).to eql(5)

    # Should fail since revision number not the latest.
    expect do
      revision = kv.update("name", "hoge", last: 3)
    end.to raise_error NATS::KeyValue::KeyWrongLastSequenceError

    # Update with correct latest.
    revision = kv.update("name", "fuga", last: revision)
    expect(revision).to eql(6)

    # Create a different key.
    revision = kv.create("age", '2038')
    expect(revision).to eql(7)

    # Get current.
    entry = kv.get("age")
    expect(entry.value).to eql('2038')
    expect(entry.revision).to eql(7)

    # Update the new key.
    revision = kv.update("age", '2039', last: revision)
    expect(revision).to eql(8)

    # Get latest.
    entry = kv.get("age")
    expect(entry.value).to eql('2039')
    expect(entry.revision).to eql(8)

    # Internally uses get msg API instead of get last msg.
    entry = kv.get("age", revision: 7)
    expect(entry.value).to eql('2038')
    expect(entry.revision).to eql(7)

    # Getting past keys with the wrong expected subject is an error.
    expect do
      kv.get("age", revision: 6)
    end.to raise_error NATS::KeyValue::KeyNotFoundError
    begin
      kv.get("age", revision: 6)
    rescue => e
      expect(e.message).to eql(
        %Q(nats: key not found: expected '$KV.TEST.age', but got '$KV.TEST.name')
      )
    end
    expect do
      kv.get("age", revision: 5)
    end.to raise_error NATS::KeyValue::KeyNotFoundError
    expect do
      kv.get("age", revision: 4)
    end.to raise_error NATS::KeyValue::KeyNotFoundError

    expect do
      entry = kv.get("name", revision=3)
      expect(entry.value).to eql('bob')
    end

    # match="nats: wrong last sequence: 8")
    expect do
      kv.create("age", '1')
    end.to raise_error NATS::KeyValue::KeyWrongLastSequenceError
    begin
      kv.create("age", '1')
    rescue => e
      expect(e.message).to eql("nats: wrong last sequence: 8")
    end

    # Now let's delete and recreate.
    kv.delete("age", last: 8)
    kv.create("age", "final")

    expect do
      kv.create("age", '1')
    end.to raise_error NATS::KeyValue::KeyWrongLastSequenceError

    begin
      kv.create("age", '1')
    rescue => e
      expect(e.message).to eql("nats: wrong last sequence: 10")
    end

    entry = kv.get("age")
    expect(entry.revision).to eql(10)

    # Purge
    status = kv.status()
    expect(status.values).to eql(9)

    kv.purge("age")
    status = kv.status()
    expect(status.values).to eql(6)

    kv.purge("name")
    status = kv.status()
    expect(status.values).to eql(2)

    expect do
      kv.get("name")
    end.to raise_error NATS::KeyValue::KeyNotFoundError

    expect do
      kv.get("age")
    end.to raise_error NATS::KeyValue::KeyNotFoundError

    nc.close
  end
end
