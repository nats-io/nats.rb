require 'rubygems'
require 'nats/client'

def usage
  puts "Usage: ruby pub.rb <subject> <msg>"; exit
end

subject, msg = ARGV
usage unless subject
msg ||= 'Hello World'

NATS.on_error { |err| puts "Server Error: #{err}"; exit! }

NATS.start { NATS.publish(subject, msg) { NATS.stop } }

puts "Published [#{subject}] : '#{msg}'"
