require 'spec_helper'
require 'monitor'

describe 'Client - Drain' do

  before(:each) do
    @s = NatsServerControl.new
    @s.start_server(true)
  end

  after(:each) do
    @s.kill_server
    sleep 1
  end

  it 'should gracefully drain a connection' do
    nc = NATS.connect
    nc2 = NATS.connect(drain_timeout: 15)

    errors = []
    nc.on_error do |e|
      errors << e
    end

    future = Future.new

    nc.on_close do |err|
      future.set_result(:closed)
    end

    wait_subs = Future.new

    t = Thread.new do
      sleep 1
      stop_sending = false
      wait_subs.wait_for(1)
      50.times do |i|
        break if nc2.closed?

        ('a'..'e').each do |subject|
          10.times do |n|
            begin
              payload = "REQ:#{subject}:#{i}"
              nc2.publish(subject, payload * 128)
            rescue => e
            end
          end
        end
        sleep 0.01
      end

      50.times do |i|
        break if nc2.closed?

        ('a'..'e').each do |subject|
          10.times do |n|
            begin
              payload = "REQ:#{subject}:#{i}"
              msg = nc2.request(subject, payload)
            rescue => e
            end
          end
        end
        sleep 0.01
      end
    end

    subs = []
    ('a'..'e').each do |subject|
      sub = nc.subscribe(subject) do |msg|
        begin
          msg.respond("OK:#{msg.data}") if msg.reply
          sleep 0.01
        rescue => e
          p e
        end
      end
      subs << sub
    end
    nc.flush
    wait_subs.set_result(:OK)

    # Let the threads start accumulating some messages.
    sleep 3

    # Start draining process asynchronously.
    nc.drain
    result = future.wait_for(30)
    expect(result).to eql(:closed)
    nc2.drain
    sleep 2
    t.exit
  end

  it 'should report drain timeout error' do
    nc = NATS.connect(drain_timeout: 0.1)

    future = Future.new

    errors = []
    nc.on_error do |e|
      errors << e
    end

    nc.on_close do |err|
      future.set_result(:closed)
    end

    wait_subs = Future.new

    subs = []
    ('a'..'e').each do |subject|
      sub = nc.subscribe(subject) do |msg|
        begin
          msg.respond("OK:#{msg.data}") if msg.reply
          sleep 0.01
        rescue => e
          p e
        end
      end
      subs << sub
    end
    nc.flush

    nc.drain
    result = future.wait_for(10)
    expect(result).to eql(:closed)
    expect(errors.first).to be_a(NATS::IO::DrainTimeoutError)
  end
end
