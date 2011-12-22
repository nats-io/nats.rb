require 'rubygems'
require 'nats/client'

["TERM", "INT"].each { |sig| trap(sig) { NATS.stop } }

def usage
  puts "Usage: ruby auto_unsub.rb <subject> [wanted=5] [send=10]"; exit
end

subject = ARGV.shift
wanted  = ARGV.shift || 5
send    = ARGV.shift || 10

usage unless subject

NATS.on_error { |err| puts "Server Error: #{err}"; exit! }

received = 0

NATS.start do
  puts "Listening on [#{subject}], auto unsubscribing after #{wanted} messages, but will send #{send}."
  NATS.subscribe(subject, :max => wanted) { |msg|
    puts "Received '#{msg}'"
    received += 1
  }
  (0...send).each { NATS.publish(subject, 'hello') }
  NATS.publish('done') { NATS.stop }
end

puts "Received #{received} messages"
