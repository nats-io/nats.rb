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

    ack = kv.put("hello", "world")
    expect(ack.seq).to eql(1)

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
    end.to raise_error(NATS::KeyValue::KeyDeletedError)

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
end
