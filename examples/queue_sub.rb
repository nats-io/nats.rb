require 'rubygems'
require 'nats/client'

["TERM", "INT"].each { |sig| trap(sig) { NATS.stop } }

def usage
  puts "Usage: ruby queue_sub.rb <subject> <queue name>"; exit
end

subject, queue_group = ARGV
usage unless subject and queue_group

NATS.on_error { |err| puts "Server Error: #{err}"; exit! }

NATS.start do
  puts "Listening on [#{subject}], queue group [#{queue_group}]"
  NATS.subscribe(subject, :queue => queue_group) { |msg| puts "Received '#{msg}'" }
end
