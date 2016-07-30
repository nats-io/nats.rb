require 'spec_helper'

describe "Client - configuration" do

  before(:each) do
    @s = NatsServerControl.new
    @s.start_server(true)
  end

  after(:each) do
    @s.kill_server
  end

  it 'should honor setting options' do
    with_em_timeout do
      NATS.start(:debug => true, :pedantic => false, :verbose => true, :reconnect => true, :max_reconnect_attempts => 100, :reconnect_time_wait => 5, :uri => 'nats://127.0.0.1:4222') do
        options = NATS.options
        expect(options).to be_a Hash
        expect(options).to have_key :debug
        expect(options[:debug]).to eql(true)
        expect(options).to have_key :pedantic
        expect(options[:pedantic]).to eql(false)
        expect(options).to have_key :verbose
        expect(options[:verbose]).to eql(true)
        expect(options).to have_key :reconnect
        expect(options[:reconnect]).to eql(true)
        expect(options).to have_key :max_reconnect_attempts
        expect(options[:max_reconnect_attempts]).to eql(100)
        expect(options).to have_key :reconnect_time_wait
        expect(options[:reconnect_time_wait]).to eql(5)
        expect(options).to have_key :uri
        expect(options[:uri].to_s).to eql('nats://127.0.0.1:4222')
        NATS.stop
      end
    end
  end

  it 'should not complain against bad subjects if pedantic mode disabled' do
    with_em_timeout do |future|
      expect do
        nc = NATS.connect(:pedantic => false, :reconnect => false)
        nc.flush do
          nc.publish('foo.>.foo', 'hello') do
            future.resume(nc)
          end
        end
      end.to_not raise_error
    end
  end

  it 'should complain against bad subjects in pedantic mode' do
    errors = []

    with_em_timeout do |future|
      nc = nil
      NATS.on_error do |e|
        errors << e
        future.resume(nc)
      end

      nc = NATS.connect(:pedantic => true, :reconnect => false)
      nc.flush do
        nc.publish('foo.>.foo', 'hello')
      end
    end

    expect(errors.count).to eql(1)
    expect(errors.first).to be_a NATS::ServerError
  end

  it 'should set verbose mode correctly' do
    NATS.start(:debug => true, :pedantic => false, :verbose => true) do
      NATS.publish('foo') { NATS.stop }
    end
  end

  it 'should have default ping options' do
    NATS.start do
      options = NATS.options
      expect(options).to be_a Hash
      expect(options).to have_key :ping_interval
      expect(options[:ping_interval]).to eql(NATS::DEFAULT_PING_INTERVAL)
      expect(options).to have_key :max_outstanding_pings
      expect(options[:max_outstanding_pings]).to eql(NATS::DEFAULT_PING_MAX)
      NATS.stop
    end
  end

  it 'should allow overrides of ping variables' do
    NATS.start(:ping_interval => 30, :max_outstanding_pings => 4) do
      options = NATS.options
      expect(options).to be_a Hash
      expect(options).to have_key :ping_interval
      expect(options[:ping_interval]).to eql(30)
      expect(options).to have_key :max_outstanding_pings
      expect(options[:max_outstanding_pings]).to eql(4)
      NATS.stop
    end
  end

  it 'should honor environment vars options' do
    ENV['NATS_VERBOSE'] = 'true'
    ENV['NATS_PEDANTIC'] = 'true'
    ENV['NATS_DEBUG'] = 'true'
    ENV['NATS_RECONNECT'] = 'true'
    ENV['NATS_FAST_PRODUCER'] = 'true'
    ENV['NATS_MAX_RECONNECT_ATTEMPTS'] = '100'
    ENV['NATS_RECONNECT_TIME_WAIT'] = '5'
    ENV['NATS_URI'] = 'nats://127.0.0.1:4222'

    NATS.start do
      options = NATS.options
      expect(options).to be_a Hash
      expect(options).to have_key :debug
      expect(options[:debug]).to eql(true)
      expect(options).to have_key :pedantic
      expect(options[:pedantic]).to eql(true)
      expect(options).to have_key :verbose
      expect(options[:verbose]).to eql(true)
      expect(options).to have_key :reconnect
      expect(options[:reconnect]).to eql(true)
      expect(options).to have_key :fast_producer_error
      expect(options[:fast_producer_error]).to eql(true)
      expect(options).to have_key :max_reconnect_attempts
      expect(options[:max_reconnect_attempts]).to eql(100)
      expect(options).to have_key :reconnect_time_wait
      expect(options[:reconnect_time_wait]).to eql(5)
      expect(options).to have_key :uri
      expect(options[:uri].to_s).to eql('nats://127.0.0.1:4222')
      NATS.stop
    end

    # Restore environment to be without options!
    ENV.delete 'NATS_VERBOSE'
    ENV.delete 'NATS_PEDANTIC'
    ENV.delete 'NATS_DEBUG'
    ENV.delete 'NATS_RECONNECT'
    ENV.delete 'NATS_FAST_PRODUCER'
    ENV.delete 'NATS_MAX_RECONNECT_ATTEMPTS'
    ENV.delete 'NATS_RECONNECT_TIME_WAIT'
    ENV.delete 'NATS_URI'
  end

  it 'should respect the reconnect parameters' do
    with_em_timeout do
      expect do
        NATS.start(:max_reconnect_attempts => 1, :reconnect_time_wait => 1) do
          NATS.stop
        end
      end.to_not raise_error
    end

    # Stop the server, make sure it can't connect and see that the time to fail make sense
    start_at = nil
    closed_at = nil
    errors = []
    with_em_timeout(5) do
      NATS.on_error do |e|
        errors << e
      end
      NATS.on_close do
        closed_at = Time.now
      end
      NATS.start(:max_reconnect_attempts => 1, :reconnect_time_wait => 1) do
        start_at = Time.now
        @s.kill_server
      end
    end
    time_diff = closed_at - start_at
    expect(errors.count).to eql(1)
    expect(errors.first).to be_a NATS::ConnectError

    # Check if the reconnect took more than the expected 4 secs...
    expect(time_diff > 1).to eql(true)
    expect(time_diff < 4).to eql(true)
  end
end
