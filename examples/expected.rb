require 'rubygems'
require 'nats/client'

["TERM", "INT"].each { |sig| trap(sig) { NATS.stop } }

def usage
  puts "Usage: ruby expected.rb <subject> [timeout (default 5 secs)] [expected (default 5)]"
  exit
end

subject  = ARGV.shift
timeout  = ARGV.shift || 5
expected = ARGV.shift || 5

usage unless subject

NATS.on_error { |err| puts "Server Error: #{err}"; exit! }

NATS.start do
  received = 0
  puts "Listening on [#{subject}]"
  puts "Will timeout in #{timeout} seconds unless #{expected} messages are received."
  sid = NATS.subscribe(subject) { |msg|
    puts "Received '#{msg}'"
    received += 1
    if received >= expected
      puts "All #{expected} messages received, exiting.."
      NATS.stop
    end
  }
  NATS.timeout(sid, timeout, :expected => expected) {
    puts "Timedout waiting for a message!"
    NATS.stop
  }
end
