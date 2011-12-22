require 'rubygems'
require 'nats/client'

["TERM", "INT"].each { |sig| trap(sig) { NATS.stop } }

def usage
  puts "Usage: ruby subtimeout.rb <subject> [timeout (default 5 secs)]"; exit
end

subject = ARGV.shift
timeout = ARGV.shift || 5

usage unless subject

NATS.on_error { |err| puts "Server Error: #{err}"; exit! }

NATS.start do
  puts "Listening on [#{subject}]"
  puts "Will timeout in #{timeout} seconds."
  sid = NATS.subscribe(subject) { |msg|
    puts "Received '#{msg}'"
    NATS.stop
  }
  NATS.timeout(sid, timeout) {
    puts "Timedout waiting for a message!"
    NATS.stop
  }
end
