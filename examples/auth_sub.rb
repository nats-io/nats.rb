
require 'rubygems'
require 'nats/client'

trap("TERM") { NATS.stop }
trap("INT")  { NATS.stop }
  
def usage
  puts "Usage: auth_sub <user> <pass> <subject>"; exit
end

user, pass, subject = ARGV
usage unless user and pass and subject

uri = "nats://#{user}:#{pass}@localhost:8222"

NATS.start(:uri => uri) do |n|
  puts "Listening on '#{subject}'"
  n.subscribe(subject) { |sub, msg| puts "Msg Received on [#{sub}] : '#{msg}'" }
end
