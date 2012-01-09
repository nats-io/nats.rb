require 'rubygems'
require 'nats/client'

["TERM", "INT"].each { |sig| trap(sig) { NATS.stop } }

NATS.on_error { |err| puts "Server Error: #{err}"; exit! }

NATS.start {

  # The helper
  NATS.subscribe('help') do |msg, reply|
    NATS.publish(reply, "I'll help!")
  end

  # Help request
  NATS.request('help') { |response|
    puts "Got a response: '#{response}'"
    NATS.stop
  }
}
