require 'rubygems'
require 'nats/client'

["TERM", "INT"].each { |sig| trap(sig) { NATS.stop } }

def usage
  puts "Usage: ruby sub.rb <subject>"; exit
end

subject = ARGV.shift
usage unless subject

NATS.on_error { |err| puts "Server Error: #{err}"; exit! }

NATS.start do
  puts "Listening on [#{subject}]"
  NATS.subscribe(subject) { |msg| puts "Received '#{msg}'" }
end
