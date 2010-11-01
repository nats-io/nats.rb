
require 'rubygems'
require 'nats/client'

trap("TERM") { NATS.stop }
trap("INT")  { NATS.stop }

NATS.on_error { |err| puts "Server Error: #{err}"; exit! }

NATS.start { |c|
  
  # The helper
  c.subscribe('help') do |sub, msg, reply|
    c.publish(reply, "I'll help!")
  end

  # Help request
  c.request('help') { |response|
    puts "Got a response: '#{response}'"
    NATS.stop
  }

}
