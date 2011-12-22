require 'rubygems'
require 'nats/client'

["TERM", "INT"].each { |sig| trap(sig) { NATS.stop } }

def usage
  puts "Usage: auth_sub <user> <pass> <subject>"; exit
end

user, pass, subject = ARGV
usage unless user and pass and subject

uri = "nats://#{user}:#{pass}@localhost:#{NATS::DEFAULT_PORT}"

NATS.on_error { |err| puts "Server Error: #{err}"; exit! }

NATS.start(:uri => uri) do
  puts "Listening on [#{subject}]"
  NATS.subscribe(subject) { |msg, _, sub| puts "Received on [#{sub}] : '#{msg}'" }
end
