
require 'rubygems'
require 'nats/client'

trap("TERM") { NATS.stop }
trap("INT")  { NATS.stop }

NATS.start { |c|
  
  test_sub = c.subscribe('test') do |sub, msg|
    puts "Test Sub is #{test_sub}"
    puts "received data on sub:#{sub}- #{msg}"
    c.unsubscribe(test_sub) # Only receive one message
  end
  
  c.publish('test', 'Hello World!')
  c.publish('test', 'Hello World 2!')  
}
