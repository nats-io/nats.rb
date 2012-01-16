require 'spec_helper'

describe 'fast producer' do

  before(:all) do
    @s = NatsServerControl.new
    @s.start_server
  end

  after(:all) do
    @s.kill_server
  end

  it 'should report the maximum outbound threshold' do
    NATS::FAST_PRODUCER_THRESHOLD.should_not be_nil
    NATS::FAST_PRODUCER_THRESHOLD.should == (10*1024*1024)
  end

  it 'should report the outstanding bytes pending' do
    data = 'hello world!'
    proto = "PUB foo  #{data.size}\r\n#{data}\r\n"
    NATS.start do
      (1..100).each { NATS.publish('foo', data) }
      NATS.pending_data_size.should == (100*proto.size)
      NATS.stop
    end
  end

  it 'should not raise an error when exceeding the threshold' do
    data = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
    EM.run do
      while (NATS.pending_data_size <= NATS::FAST_PRODUCER_THRESHOLD) do
        NATS.publish('foo', data)
      end
      EM.stop
    end
  end

  it 'should raise an error when exceeding the threshold if enabled' do
    data = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
    EM.run do
      c = NATS.connect(:fast_producer_error => true)
      expect do
        while (c.pending_data_size <= NATS::FAST_PRODUCER_THRESHOLD) do
          c.publish('foo', data)
        end
      end.to raise_error(NATS::ClientError)
      c.close
      EM.stop
    end
  end

  it 'should raise an error when exceeding the threshold if ENV enabled' do
    data = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
    EM.run do
      ENV['NATS_FAST_PRODUCER'] = 'true'
      c = NATS.connect
      expect do
        while (c.pending_data_size <= NATS::FAST_PRODUCER_THRESHOLD) do
          c.publish('foo', data)
        end
      end.to raise_error(NATS::ClientError)
      c.close
      EM.stop
    end
  end

end
