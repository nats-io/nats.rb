
require 'rubygems'
require 'nats/client'

trap("TERM") { NATS.stop }
trap("INT")  { NATS.stop }

NATS.on_error { |err| puts "Server Error: #{err}"; exit! }

NATS.start { |c|

  test_sub = c.subscribe('test') do |msg, _, sub|
    puts "Test Sub is #{test_sub}"
    puts "received data on sub:#{sub}- #{msg}"
    c.unsubscribe(test_sub) # Only receive one message
    NATS.stop
  end

  c.publish('test', 'Hello World!')
  c.publish('test', 'Hello World 2!')
}
