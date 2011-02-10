require 'rubygems'
require 'nats/client'

def usage
  puts "Usage: pub.rb <user> <pass> <subject> <msg>"; exit
end

user, pass, subject, msg = ARGV
usage unless user and pass and subject

# Default
msg ||= 'Hello World'

uri = "nats://#{user}:#{pass}@localhost:#{NATS::DEFAULT_PORT}"

NATS.on_error { |err| puts "Server Error: #{err}"; exit! }

NATS.start(:uri => uri) do
  NATS.publish(subject, msg)
  NATS.stop
end

puts "Published on [#{subject}] : '#{msg}'"
