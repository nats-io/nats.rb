
require 'rubygems'
require 'nats/client'

trap("TERM") { NATS.stop }
trap("INT")  { NATS.stop }

NATS.on_error { |err| puts "Server Error: #{err}"; exit! }

NATS.start {
  
  # The helper
  NATS.subscribe('help') do |sub, msg, reply|
    NATS.publish(reply, "I'll help!")
  end

  # Help request
  NATS.request('help') { |response|
    puts "Got a response: '#{response}'"
    NATS.stop
  }

}
