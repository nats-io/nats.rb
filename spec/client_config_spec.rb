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
    NATS.start(:debug => true, :pedantic => false, :verbose => true) do
      options = NATS.options
      options.should be_an_instance_of Hash
      options.should have_key :debug
      options[:debug].should be_true
      options.should have_key :pedantic
      options[:pedantic].should be_false
      options.should have_key :verbose
      options[:verbose].should be_true
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

end
