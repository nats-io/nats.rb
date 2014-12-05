require 'spec_helper'
require 'nats/server/const'

describe 'NATS Protocol' do

  context 'sub' do

    it 'should match simple sub' do
      str = "SUB foo 1\r\n"
      NATSD::SUB_OP =~ str
      $1.should == 'foo'
      $3.should == nil
      $4.should == '1'
    end

    it 'should not care about case' do
      str = "sub foo 1\r\n"
      NATSD::SUB_OP =~ str
      $1.should == 'foo'
      $3.should == nil
      $4.should == '1'
    end

    it 'should match a queue group' do
      str = "SUB foo bargroup 1\r\n"
      NATSD::SUB_OP =~ str
      $1.should == 'foo'
      $3.should == 'bargroup'
      $4.should == '1'
    end

    it 'should not care about extra spaces' do
      str = "SUB    foo  bargroup   1\r\n"
      NATSD::SUB_OP =~ str
      $1.should == 'foo'
      $3.should == 'bargroup'
      $4.should == '1'
    end

    it 'should care about extra spaces at end' do
      str = "SUB    foo  bargroup   1   \r\n"
      (NATSD::SUB_OP =~ str).should be_falsey
    end

    it 'should properly match first one when multiple present' do
      str = "SUB foo 1\r\nSUB bar 2\r\nSUB baz 3\r\n"
      NATSD::SUB_OP =~ str
      $1.should == 'foo'
      $3.should == nil
      $4.should == '1'
      $'.should == "SUB bar 2\r\nSUB baz 3\r\n"
    end

    it 'should not tolerate spaces after \r\n' do
      str = "SUB foo 1\r\n   SUB bar 2\r\nSUB baz 3\r\n"
      NATSD::SUB_OP =~ str
      str = $'
      NATSD::SUB_OP =~ str
      $1.should == nil
      $3.should == nil
      $4.should == nil
    end

  end

  context 'unsub' do

    it 'should process simple unsub' do
      str = "UNSUB 1\r\n"
      NATSD::UNSUB_OP =~ str
      $1.should == '1'
    end

    it 'should not care about case' do
      str = "unsub 1\r\n"
      NATSD::UNSUB_OP =~ str
      $1.should == '1'
    end

    it 'should tolerate extra spaces' do
      str = "UNSUB   1\r\n"
      NATSD::UNSUB_OP =~ str
      $1.should == '1'
    end

    it 'should properly match first one when multiple present' do
      str = "UNSUB 1\r\nUNSUB 2\r\nSUB foo 2\r\n"
      NATSD::UNSUB_OP =~ str
      $1.should == '1'
      $'.should == "UNSUB 2\r\nSUB foo 2\r\n"
    end

    it 'should properly parse auto-unsubscribe message count' do
      str = "UNSUB 1 22\r\n"
      NATSD::UNSUB_OP =~ str
      $1.should == '1'
      $2.should be
      $3.to_i.should == 22
    end

    it 'should properly parse auto-unsubscribe message count with extra spaces' do
      str = "UNSUB  1   22\r\n"
      NATSD::UNSUB_OP =~ str
      $1.should == '1'
      $2.should be
      $3.to_i.should == 22
    end

  end

  context 'pub' do

    it 'should process simple pub' do
      str = "PUB foo 2\r\nok\r\n"
      NATSD::PUB_OP =~ str
      $1.should == 'foo'
      $3.should == nil
      $4.to_i.should == 2
      $'.should == "ok\r\n"
    end

    it 'should not care about case' do
      str = "pub foo 2\r\nok\r\n"
      NATSD::PUB_OP =~ str
      $1.should == 'foo'
      $3.should == nil
      $4.to_i.should == 2
      $'.should == "ok\r\n"
    end

    it 'should process reply semantics' do
      str = "PUB foo bar 2\r\nok\r\n"
      NATSD::PUB_OP =~ str
      $1.should == 'foo'
      $3.should == 'bar'
      $4.to_i.should == 2
      $'.should == "ok\r\n"
    end

    it 'should not care about extra spaces' do
      str = "PUB  foo     bar  2\r\nok\r\n"
      NATSD::PUB_OP =~ str
      $1.should == 'foo'
      $3.should == 'bar'
      $4.to_i.should == 2
      $'.should == "ok\r\n"
    end

    it 'should care about extra spaces at end' do
      str = "PUB  foo     bar  2   \r\nok\r\n"
      (NATSD::PUB_OP =~ str).should be_falsey
    end

    it 'should properly match first one when multiple present' do
      str = "PUB foo bar  2\r\nok\r\nPUB bar 11\r\nHello World\r\n"
      NATSD::PUB_OP =~ str
      $1.should == 'foo'
      $3.should ==  'bar'
      $4.to_i.should == 2
      $'.should == "ok\r\nPUB bar 11\r\nHello World\r\n"
    end

  end

  context 'misc' do

    it 'should process ping requests' do
      str = "PING\r\n"
      (NATSD::PING =~ str).should be_truthy
    end

    it 'should process ping requests with spaces' do
      str = "PING  \r\n"
      (NATSD::PING =~ str).should be_truthy
    end

    it 'should process pong responses' do
      str = "PONG\r\n"
      (NATSD::PONG =~ str).should be_truthy
    end

    it 'should process ping responses with spaces' do
      str = "PONG  \r\n"
      (NATSD::PONG =~ str).should be_truthy
    end

    it 'should process info requests' do
      str = "INFO\r\n"
      (NATSD::INFO =~ str).should be_truthy
    end

    it 'should process info requests with spaces' do
      str = "INFO  \r\n"
      (NATSD::INFO =~ str).should be_truthy
    end

    it 'should process connect requests' do
      str = "CONNECT {\"user\":\"derek\"}\r\n"
      NATSD::CONNECT =~ str
      $1.should == "{\"user\":\"derek\"}"
    end

  end

  context 'mixed' do

    it 'should process multiple commands in one buffer properly' do
      str = "PUB foo bar  2\r\nok\r\nSUB bar 22\r\nPUB bar 11\r\nHello World\r\n"
      NATSD::PUB_OP =~ str
      $1.should == 'foo'
      $3.should ==  'bar'
      $4.to_i.should == 2
      $'.should == "ok\r\nSUB bar 22\r\nPUB bar 11\r\nHello World\r\n"
      str = $'
      str = str.slice($4.to_i + NATSD::CR_LF_SIZE, str.bytesize)
      NATSD::SUB_OP =~ str
      $1.should == 'bar'
      $3.should == nil
      $4.should == '22'
    end

  end

  context 'client' do

    it 'should process ping and pong responsess' do
      str = "PING\r\n"
      (NATS::PING =~ str).should be_truthy
      str = "PING \r\n"
      (NATS::PING =~ str).should be_truthy
      str = "PONG\r\n"
      (NATS::PONG =~ str).should be_truthy
      str = "PONG \r\n"
      (NATS::PONG =~ str).should be_truthy
    end

    it 'should process ok responses' do
      str = "+OK\r\n"
      (NATS::OK =~ str).should be_truthy
      str = "+OK \r\n"
      (NATS::OK =~ str).should be_truthy
    end

    it 'should process err responses' do
      str = "-ERR 'string too long'\r\n"
      (NATS::ERR =~ str).should be_truthy
      $1.should == "'string too long'"
    end

    it 'should process messages' do
      str = "MSG foo 2 11\r\nHello World\r\n"
      NATS::MSG =~ str
      $1.should == 'foo'
      $2.should == '2'
      $4.should == nil
      $5.to_i.should == 11
      $'.should == "Hello World\r\n"
    end

    it 'should process messages with a reply' do
      str = "MSG foo  2 reply_to_me 11\r\nHello World\r\n"
      NATS::MSG =~ str
      $1.should == 'foo'
      $2.should == '2'
      $4.should == 'reply_to_me'
      $5.to_i.should == 11
      $'.should == "Hello World\r\n"
    end

    it 'should process messages with extra spaces' do
      str = "MSG    foo  2     reply_to_me  11\r\nHello World\r\n"
      NATS::MSG =~ str
      $1.should == 'foo'
      $2.should == '2'
      $4.should == 'reply_to_me'
      $5.to_i.should == 11
      $'.should == "Hello World\r\n"
    end

    it 'should process multiple messages in a single read properly' do
      str = "MSG foo 2 11\r\nHello World\r\nMSG foo  2 reply_to_me 2\r\nok\r\n"
      NATS::MSG =~ str
      $1.should == 'foo'
      $2.should == '2'
      $4.should == nil
      $5.to_i.should == 11
      str = $'
      str = str.slice($5.to_i + NATSD::CR_LF_SIZE, str.bytesize)
      NATS::MSG =~ str
      $1.should == 'foo'
      $2.should == '2'
      $4.should == 'reply_to_me'
      $5.to_i.should == 2
      $'.should == "ok\r\n"
    end

  end

end
