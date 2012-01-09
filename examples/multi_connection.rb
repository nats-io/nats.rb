require 'rubygems'
require 'nats/client'

["TERM", "INT"].each { |sig| trap(sig) { NATS.stop } }

NATS.on_error { |err| puts "Server Error: #{err}"; exit! }

NATS.start {
  NATS.subscribe('test') do |msg, reply, sub|
    puts "received data on sub:#{sub} - #{msg}"
    NATS.stop
  end

  # Form a second connection to send message on
  NATS.connect { |nc| nc.publish('test', 'Hello World!') }
}
