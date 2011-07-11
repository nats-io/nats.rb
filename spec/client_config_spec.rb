require 'spec_helper'

describe "client configuration" do

  before(:all) do
    @s = NatsServerControl.new
    @s.start_server
  end

  after(:all) do
    @s.kill_server
  end

  it 'should honor setting options' do
    NATS.start(:debug => true, :pedantic => false, :verbose => true, :reconnect => true, :max_reconnect_attempts => 100, :reconnect_time_wait => 5, :uri => 'nats://127.0.0.1:4222') do
      options = NATS.options
      options.should be_an_instance_of Hash
      options.should have_key :debug
      options[:debug].should be_true
      options.should have_key :pedantic
      options[:pedantic].should be_false
      options.should have_key :verbose
      options[:verbose].should be_true
      options.should have_key :reconnect
      options[:reconnect].should be_true 
      options.should have_key :max_reconnect_attempts
      options[:max_reconnect_attempts].should == 100 
      options.should have_key :reconnect_time_wait
      options[:reconnect_time_wait].should == 5
      options.should have_key :uri
      options[:uri].should.to_s == 'nats://127.0.0.1:4222'
      NATS.stop
    end
  end

  it 'should complain against bad subjects in pedantic mode' do
    expect do
      NATS.start(:pedantic => false) do
        NATS.publish('foo.>.foo') { NATS.stop }
      end
    end.to_not raise_error

    expect do
      NATS.start(:pedantic => true) do
        NATS.publish('foo.>.foo') { NATS.stop }
      end
    end.to raise_error
  end

  it 'should set verbose mode correctly' do
    NATS.start(:debug => true, :pedantic => false, :verbose => true) do
      NATS.publish('foo') { NATS.stop }
    end
  end

  it 'should honor environment vars options' do
    ENV['NATS_VERBOSE'] = 'true'
    ENV['NATS_PEDANTIC'] = 'true'
    ENV['NATS_DEBUG'] = 'true'
    ENV['NATS_RECONNECT'] = 'true'
    ENV['NATS_MAX_RECONNECT_ATTEMPTS'] = '100'
    ENV['NATS_RECONNECT_TIME_WAIT'] = '5'
    ENV['NATS_URI'] = 'nats://127.0.0.1:4222'
 
    NATS.start do
      options = NATS.options
      options.should be_an_instance_of Hash
      options.should have_key :debug
      options[:debug].should be_true
      options.should have_key :pedantic
      options[:pedantic].should be_true
      options.should have_key :verbose
      options[:verbose].should be_true
      options.should have_key :reconnect
      options[:reconnect].should be_true 
      options.should have_key :max_reconnect_attempts
      options[:max_reconnect_attempts].should == 100 
      options.should have_key :reconnect_time_wait
      options[:reconnect_time_wait].should == 5
      options.should have_key :uri
      options[:uri].to_s.should == 'nats://127.0.0.1:4222'
      NATS.stop
    end

    # Restore environment to be without options!
    ENV.delete 'NATS_VERBOSE'
    ENV.delete 'NATS_PEDANTIC'
    ENV.delete 'NATS_DEBUG'
    ENV.delete 'NATS_RECONNECT'
    ENV.delete 'NATS_MAX_RECONNECT_ATTEMPTS'
    ENV.delete 'NATS_RECONNECT_TIME_WAIT'
    ENV.delete 'NATS_URI'
  end

end
