require 'fiber'
require 'nats/client'

["TERM", "INT"].each { |sig| trap(sig) { EM.stop } }

NATS.on_error { |err| puts "Server Error: #{err}"; exit! }

NATS.start {

  NATS.subscribe('help') do |msg, reply|
    puts "[Received]: <<- #{msg}"
    NATS.publish(reply, "I'll help! - #{msg}")
  end

  NATS.subscribe('slow') do |msg, reply|
    puts "[Received]: <<- #{msg}"
    EM.add_timer(1) { NATS.publish(reply, "I'll help! - #{msg}") }
  end

  10.times do |n|
    NATS.subscribe('hi') do |msg, reply|
      NATS.publish(reply, "Hello World! - id:#{n}")
    end
  end

  Fiber.new do
    # Requests work synchronously within the same Fiber
    # returning the message when done.
    response = NATS.request('help', 'foo')
    puts "[Response]: ->> '#{response}'"

    # Specifying a custom timeout to give up waiting for
    # a response.
    response = NATS.request('slow', 'bar', timeout: 2)
    if response.nil?
      puts "No response after 2 seconds..."
    else
      puts "[Response]: ->> '#{response}'"
    end

    # Can gather multiple responses with the same request
    # which will then return a collection with the responses
    # that were received before the timeout.
    responses = NATS.request('hi', 'quux', max: 10, timeout: 1)
    responses.each_with_index do |response, i|
      puts "[Response# #{i}]: ->> '#{response}'"
    end
    
    # If no replies then an empty collection is returned.
    responses = NATS.request('nowhere', '', max: 10, timeout: 2)
    if responses.any?
      puts "Got #{responses.count} responses"
    else
      puts "No response after 2 seconds..."
    end

    NATS.stop
  end.resume

  # Multiple fibers can make requests concurrently
  # under the same Eventmachine loop.
  Fiber.new do
    10.times do |n|
      response = NATS.request('help', "help.#{n}")
      puts "[Response]: ->> '#{response}'"
    end
  end.resume
}
