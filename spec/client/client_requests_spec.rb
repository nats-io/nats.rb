require 'spec_helper'

describe 'Client - requests' do

  before(:each) do
    @s = NatsServerControl.new
    @s.start_server(true)
  end

  after(:each) do
    @s.kill_server
  end

  it 'should receive responses using single subscription for requests' do
    msgs = []
    received = false
    nats = nil
    NATS.start do |nc|
      nats = nc
      nc.subscribe('need_help') do |msg, reply|
        nc.publish(reply, "help-#{msg}")
      end

      Fiber.new do
        msgs << nc.request('need_help', 'yyy')
        msgs << nc.request('need_help', 'zzz')
        received = true
        NATS.stop
      end.resume

      timeout_nats_on_failure
    end
    expect(received).to eql(true)
    expect(msgs.first).to eql('help-yyy')
    expect(msgs.last).to eql('help-zzz')
    resp_map = nats.instance_variable_get('@resp_map')
    expect(resp_map.keys.count).to eql(0)
  end

  it 'should receive a response from a request' do
    received = false
    nats = nil
    NATS.start do |nc|
      nats = nc
      nc.subscribe('need_help') do |msg, reply|
        expect(msg).to eql('yyy')
        nc.publish(reply, 'help')
      end

      Fiber.new do
        response = nc.request('need_help', 'yyy')
        received = true
        expect(response).to eql('help')
        NATS.stop
      end.resume

      timeout_nats_on_failure
    end
    expect(received).to eql(true)
    resp_map = nats.instance_variable_get('@resp_map')
    expect(resp_map.keys.count).to eql(0)
  end

  it 'should perform similar using class mirror functions' do
    received = false
    NATS.start do
      s = NATS.subscribe('need_help') do |msg, reply|
        expect(msg).to eql('yyy')
        NATS.publish(reply, 'help')
        NATS.unsubscribe(s)
      end

      Fiber.new do
        response = NATS.request('need_help', 'yyy')
        received = true
        expect(response).to eql('help')
        NATS.stop
      end.resume

      timeout_nats_on_failure
    end
    expect(received).to eql(true)
  end

  it 'should be possible to gather multiple responses before a timeout' do
    nats = nil
    responses = []
    NATS.start do |nc|
      nats = nc
      10.times do |n|
        nc.subscribe('need_help') do |msg, reply|
          expect(msg).to eql('yyy')
          nc.publish(reply, "help-#{n}")
        end
      end

      Fiber.new do
        responses = nc.request('need_help', 'yyy', max: 5, timeout: 1)
        NATS.stop
      end.resume

      timeout_nats_on_failure(2)
    end
    expect(responses.count).to eql(5)

    # NOTE: Behavior change here in NATS v1.2.0, now responses
    # are not in the same order as the subscriptions.
    # responses.each_with_index do |response, i|
    #   expect(response).to eql("help-#{i}")
    # end
    expect(responses.count).to eql(5)

    resp_map = nats.instance_variable_get('@resp_map')
    expect(resp_map.keys.count).to eql(0)
  end

  it 'should be possible to gather single response from a queue group before a timeout' do
    nats = nil
    responses = []
    NATS.start do |nc|
      nats = nc
      10.times do |n|
        nc.subscribe('need_help', queue: 'worker') do |msg, reply|
          expect(msg).to eql('yyy')
          nc.publish(reply, "help")
        end
      end

      Fiber.new do
        responses = nc.request('need_help', 'yyy', max: 5, timeout: 1)
        NATS.stop
      end.resume

      timeout_nats_on_failure(2)
    end
    expect(responses.count).to eql(1)
    responses.each_with_index do |response, i|
      expect(response).to eql("help")
    end
    resp_map = nats.instance_variable_get('@resp_map')
    expect(resp_map.keys.count).to eql(0)
  end

  it 'should be possible to gather as many responses as possible before the timeout' do
    nats = nil
    responses = []
    NATS.start do |nc|
      nats = nc
      nc.subscribe('need_help') do |msg, reply|
        3.times do |n|
          nc.publish(reply, "help-#{n}")
        end

        EM.add_timer(1) do
          3.upto(10).each do |n|
            nc.publish(reply, "help-#{n}")
          end
        end
      end

      Fiber.new do
        responses = nc.request('need_help', 'yyy', max: 5, timeout: 0.5)
        NATS.stop
      end.resume

      timeout_nats_on_failure(2)
    end

    # Expected 5 but only 3 made it.
    expect(responses.count).to eql(3)
    responses.each_with_index do |response, i|
      expect(response).to eql("help-#{i}")
    end
    resp_map = nats.instance_variable_get('@resp_map')
    expect(resp_map.keys.count).to eql(0)
  end

  it 'should return empty array in case waited many responses but got none before timeout' do
    nats = nil
    responses = []
    NATS.start do |nc|
      nats = nc
      Fiber.new do
        responses = nc.request('need_help', 'yyy', max: 5, timeout: 0.5)
        NATS.stop
      end.resume

      timeout_nats_on_failure(2)
    end

    expect(responses.count).to eql(0)
    resp_map = nats.instance_variable_get('@resp_map')
    expect(resp_map.keys.count).to eql(0)
  end

  it 'should return nil in case waited single response but got none before timeout' do
    response = nil
    NATS.start do |nc|
      Fiber.new do
        response = nc.request('need_help', 'yyy')
        NATS.stop
      end.resume

      timeout_nats_on_failure(2)
    end
    expect(response).to eql(nil)
  end

  it 'should fail if not wrapped in a fiber' do
    NATS.start do
      expect do
        NATS.request('need_help', 'yyy')
      end.to raise_error(FiberError)
      NATS.stop
    end
  end
end
