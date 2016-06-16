require 'spec_helper'

describe 'Client - fast producer' do

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

  it 'should not raise an error when exceeding the threshold by default' do
    data = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
    EM.run do
      nc = NATS.connect
      expect do
        # Put as many commands in pending as we can and confirm that
        # hitting the maximum threshold does not yield an error.
        while (nc.pending_data_size <= NATS::FAST_PRODUCER_THRESHOLD) do
          nc.publish('foo', data)
        end
      end.not_to raise_error
      nc.close
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

  context 'with NATS_FAST_PRODUCER enabled from ENV' do
    before(:all) do
      ENV['NATS_FAST_PRODUCER'] = 'true'
    end

    after(:all) do
      ENV.delete 'NATS_FAST_PRODUCER'
    end

    it 'should raise an error when exceeding the threshold' do
      data = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
      EM.run do
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
end
