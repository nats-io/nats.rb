require 'rubygems'
require 'nats/client'

trap("TERM") { NATS.stop }
trap("INT")  { NATS.stop }

NATS.on_error { |err| puts "Server Error: #{err}"; exit! }

NATS.start {

  NATS.subscribe('test') do |msg, reply, sub|
    puts "received data on sub:#{sub} - #{msg}"
    NATS.stop
  end

  NATS.publish('test', 'Hello World!')
}
