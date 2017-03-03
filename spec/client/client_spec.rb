require 'spec_helper'

describe 'Client - specification' do

  before(:each) do
    @s = NatsServerControl.new
    @s.start_server(true)
  end

  after(:each) do
    @s.kill_server
  end

  it "should complain if it can't connect to server when not running" do
    errors = []
    with_em_timeout do
      NATS.on_error do |e|
        errors << e
      end

      NATS.connect(:uri => 'nats://127.0.0.1:3222')
    end
    expect(errors.count).to eql(1)
    expect(errors.first).to be_a(NATS::ConnectError)
  end

  it 'should complain if NATS.start is called without EM running and no block was given', :jruby_excluded do
    expect(EM.reactor_running?).to eql(false)
    expect { NATS.start }.to raise_error(NATS::Error)
    expect(NATS.connected?).to eql(false)
  end

  it 'should perform basic block start and stop' do
    NATS.start { NATS.stop }
  end

  it 'should signal connected state' do
    NATS.start do
      expect(NATS.connected?).to eql(true)
      NATS.stop
    end
  end

  it 'should have err_cb cleared after stop' do
    NATS.start do
      NATS.on_error { puts 'err' }
      NATS.stop
    end
    expect(NATS.err_cb).to eql(nil)
  end

  it 'should raise NATS::ServerError on error replies from NATS Server' do
    skip 'trying to unsubscribe non existant subscription does not send -ERR back in pedantic mode on gnatsd'

    expect do
      NATS.start(:pedantic => true) do
        NATS.unsubscribe(10000)
        NATS.publish('done') { NATS.stop }
      end
    end.to raise_error(NATS::ServerError)
  end

  it 'should do publish without payload and with opt_reply without error' do
    NATS.start { |nc|
      nc.publish('foo')
      nc.publish('foo', 'hello')
      nc.publish('foo', 'hello', 'reply')
      NATS.stop
    }
  end

  it 'should not complain when publishing to nil' do
    errors = []
    with_em_timeout do
      NATS.on_error do |e|
        errors << e
      end
      NATS.start do
        NATS.publish(nil)
        NATS.publish(nil, 'hello')
      end
    end
    expect(errors.count).to eql(0)
  end

  it 'should receive a sid when doing a subscribe' do
    sid = nil
    errors = []
    with_em_timeout do
      NATS.on_error do |e|
        errors << e
      end
      NATS.connect do |nc|
        sid = nc.subscribe('foo')
      end
    end
    expect(errors.count).to eql(0)
    expect(sid).to_not eql(nil)
  end

  it 'should receive a sid when doing a request' do
    sid = nil
    errors = []
    with_em_timeout do
      NATS.on_error do |e|
        errors << e
      end
      NATS.start do |nc|
        sid = nc.request('foo')
      end
    end
    expect(sid).to_not eql(nil)
  end

  it 'should receive a message that it has a subscription to' do
    received = false
    received_msg = nil
    with_em_timeout do
      NATS.start do |nc|
        nc.subscribe('foo') do |msg|
          received = true
          received_msg = msg
          NATS.stop
        end
        nc.publish('foo', 'xxx')
      end
    end
    expect(received).to eql(true)
    expect(received_msg).to eql('xxx')
  end

  it 'should receive a message that it has a wildcard subscription to' do
    received = false
    with_em_timeout do
      NATS.start do |nc|
        nc.subscribe('*') do |msg|
          received = true
          expect(msg).to eql('xxx')
          NATS.stop
        end
        nc.publish('foo', 'xxx')
      end
    end
    expect(received).to eql(true)
  end

  it 'should not receive a message that it has unsubscribed from' do
    sid = nil
    received = 0
    msgs = []
    with_em_timeout do
      NATS.start do |nc|
        sid = nc.subscribe('*') do |msg|
          received += 1
          msgs << msg
          nc.unsubscribe(sid)
        end
        nc.publish('foo', 'xxx')
      end
    end
    expect(received).to eql(1)
    expect(msgs.first).to eql('xxx')
  end

  it 'should receive a response from a request' do
    received = false
    NATS.start do |nc|
      nc.subscribe('need_help') do |msg, reply|
        expect(msg).to eql('yyy')
        nc.publish(reply, 'help')
      end
      nc.request('need_help', 'yyy') do |response|
        received = true
        expect(response).to eql('help')
        NATS.stop
      end
      timeout_nats_on_failure
    end
    expect(received).to eql(true)
  end

  it 'should perform similar using class mirror functions' do
    received = false
    NATS.start do
      s = NATS.subscribe('need_help') do |msg, reply|
        expect(msg).to eql('yyy')
        NATS.publish(reply, 'help')
        NATS.unsubscribe(s)
      end
      r = NATS.request('need_help', 'yyy') do |response|
        received = true
        expect(response).to eql('help')
        NATS.unsubscribe(r)
        NATS.stop
      end
      timeout_nats_on_failure
    end
    expect(received).to eql(true)
  end

  it 'should return inside closure on publish when server received msg' do
    received_pub_closure = false
    NATS.start {
      NATS.publish('foo') {
        received_pub_closure = true
        NATS.stop
      }
      timeout_nats_on_failure
    }
    expect(received_pub_closure).to eql(true)
  end

  it 'should return inside closure in ordered fashion when server received msg' do
    replies = []
    expected = []
    received_pub_closure = false
    NATS.start {
      (1..100).each { |i|
        expected << i
        NATS.publish('foo') { replies << i }
      }
      NATS.publish('foo') {
        received_pub_closure = true
        NATS.stop
      }
      timeout_nats_on_failure
    }
    expect(received_pub_closure).to eql(true)
    expect(replies).to eql(expected)
  end

  it "should be able to start and use a new connection inside of start block" do
    new_conn = nil
    received = false
    NATS.start {
      NATS.subscribe('foo') { received = true; NATS.stop }
      new_conn = NATS.connect do
        new_conn.publish('foo', 'hello')
      end
      timeout_nats_on_failure(5)
    }
    expect(new_conn).to_not eql(nil)
    expect(received).to eql(true)
  end

  it 'should allow proper request/reply across multiple connections' do
    new_conn = nil
    received_request = false
    received_reply = false

    NATS.start {
      new_conn = NATS.connect
      new_conn.subscribe('test_conn_rr') do |msg, reply|
        received_request = true
        new_conn.publish(reply)
      end
      new_conn.on_connect do
        NATS.request('test_conn_rr') do
          received_reply = true
          NATS.stop
        end
      end
      timeout_nats_on_failure
    }
    expect(new_conn).to_not eql(nil)
    expect(received_request).to eql(true)
    expect(received_reply).to eql(true)
  end

  it 'should complain if NATS.start called without a block when we would need to start EM' do
    expect do
      NATS.start
      NATS.stop
    end.to raise_error(NATS::Error)
  end

  it 'should not complain if NATS.start called without a block when EM is running already', :jruby_excluded do
    EM.run do
      expect do
        NATS.start
        EM.next_tick { NATS.stop { EM.stop } }
      end.to_not raise_error
    end
  end

  it 'should use default url if passed uri is nil' do
    NATS.start(:uri => nil) {  NATS.stop }
  end

  it 'should not complain about publish to nil unless in pedantic mode' do
    NATS.start {
      NATS.publish(nil, 'Hello!')
      NATS.stop
    }
  end

  it 'should allow proper unsubscribe from within blocks' do
    received = 0
    NATS.start do
      sid = NATS.subscribe('foo') { |msg|
        received += 1
        expect(sid).to_not eql(true)
        NATS.unsubscribe(sid)
      }
      NATS.publish('foo')
      NATS.publish('foo') { NATS.stop }
    end
    expect(received).to eql(1)
  end

  it 'should not call error handler for double unsubscribe unless in pedantic mode' do
    got_error = false
    NATS.on_error { got_error = true; NATS.stop }
    NATS.start do
      s = NATS.subscribe('foo')
      NATS.unsubscribe(s)
      NATS.unsubscribe(s)
      NATS.publish('flush') { NATS.stop }
    end
    expect(got_error).to eql(false)
  end

  it 'should call error handler for double unsubscribe if in pedantic mode' do
    skip 'double unsubscribe in gnatsd does not send -ERR back'

    got_error = false
    NATS.on_error { got_error = true; NATS.stop }
    NATS.start(:pedantic => true) do
      s = NATS.subscribe('foo')
      NATS.unsubscribe(s)
      NATS.unsubscribe(s)
      NATS.publish('flush') { NATS.stop }
    end
    expect(got_error).to eql(true)
  end

  it 'should monitor inbound and outbound messages and bytes' do
    msg = 'Hello World!'
    NATS.start do |c|
      NATS.subscribe('foo')
      NATS.publish('foo', msg)
      NATS.publish('bar', msg)
      NATS.flush do
        expect(c.msgs_sent).to eql(2)
        expect(c.msgs_received).to eql(1)
        expect(c.bytes_received).to eql(msg.size)
        expect(c.bytes_sent).to eql(msg.size * 2)
        NATS.stop
      end
    end
  end

  it 'should receive a pong from a server after ping_interval' do
    NATS.start(:ping_interval => 0.75) do
      expect(NATS.client.pongs_received).to eql(0)
      EM.add_timer(1) do
        expect(NATS.client.pongs_received).to eql(1)
        NATS.stop
      end
    end
  end

  it 'should disconnect from the server when pongs not received' do
    EM.run do
      NATS.connect(:ping_interval => 0.1, :max_outstanding_pings => 1, :reconnect => false) do |c|
        c.on_error { NATS.stop }
        def c.process_pong
          # override to not process counters
        end
      end
      EM.add_timer(0.5) do
        expect(NATS.connected?).to eql(false)
        EM.stop
      end
    end
  end

  it 'should stop the ping timer when disconnected or closed' do
    EM.run do
      $pings_received = 0
      NATS.connect(:ping_interval => 0.1) do |c|
        def c.send_ping
          $pings_received += 1
          close
        end
      end
      EM.add_timer(0.5) do
        expect($pings_received).to eql(1)
        EM.stop
      end
    end
  end

  it 'should allowing setting name for the client on connect' do
    with_em_timeout do
      connect_command = "CONNECT {\"verbose\":false,\"pedantic\":false,\"lang\":\"#{NATS::LANG}\",\"version\":\"#{NATS::VERSION}\",\"protocol\":#{NATS::PROTOCOL_VERSION},\"name\":\"hello\"}\r\n"
      conn = NATS.connect(:name => "hello")
      expect(conn.connect_command).to eql(connect_command)
    end
  end

  it 'should not repeat SUB commands when connecting' do
    pending_commands = "CONNECT {\"verbose\":false,\"pedantic\":true,\"lang\":\"#{NATS::LANG}\",\"version\":\"#{NATS::VERSION}\",\"protocol\":#{NATS::PROTOCOL_VERSION}}\r\n"
    pending_commands += "PING\r\n"
    pending_commands += "SUB hello  2\r\nSUB hello  3\r\nSUB hello  4\r\nSUB hello  5\r\nSUB hello  6\r\n"

    msgs = []
    expect do
      EM.run do
        EM.add_timer(1) do
          expect(msgs.count).to eql(5)
          EM.stop
        end
        conn = NATS.connect(:pedantic => true)
        expect(conn).to receive(:send_data).once.with(pending_commands).and_call_original

        5.times do
          conn.subscribe("hello") do |msg|
            msgs << msg
          end
        end

        # Expect INFO followed by PONG response
        expect(conn).to receive(:receive_data).at_least(:twice).and_call_original
        expect(conn).to receive(:send_data).once.with("PUB hello  5\r\nworld\r\n").and_call_original
        conn.flush do
          # Once we connected already and received PONG back,
          # we should be able to publish here.
          conn.publish("hello", "world")
        end
      end
    end.to_not raise_error
  end

  it 'should accept the same option set twice' do
    opts = {:uri => 'nats://127.0.0.1:4222'}
    NATS.start(opts) { NATS.stop }
    NATS.start(opts) { NATS.stop }
  end

  describe '#create_inbox' do
    it 'create the expected format' do
      expect(NATS.create_inbox).to match(/_INBOX\.[a-f0-9]{12}/)
    end

    context 'when Kernel.srand is regularly reset to the same value' do
      it 'should generate a unique inbox name' do
        Kernel.srand 5555
        first_inbox_name = NATS.create_inbox

        Kernel.srand 5555
        second_inbox_name = NATS.create_inbox

        expect(second_inbox_name).to_not eq(first_inbox_name)
      end
    end
  end
end
