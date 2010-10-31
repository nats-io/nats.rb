#!/usr/bin/env ruby

require 'rubygems'
require 'nats/client'

trap("TERM") { EM.stop }
trap("INT")  { EM.stop }

NATS.start { |c|
  
  test_sub = c.subscribe('test') do |sub, msg|
    puts "Test Sub is #{test_sub}"
    puts "received data on sub:#{sub}- #{msg}"
    c.unsubscribe(test_sub)
  end
  
  c.publish('test', 'Hello World!')
  c.publish('test', 'Hello World 2!')  
}
