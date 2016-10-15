require 'nats/io/client'

nats = NATS::IO::Client.new

nats.connect(:servers => ["nats://127.0.0.1:4222"]) do |nc|
  puts "Connected to #{nc.connected_server}"
end

# Simple subscriber
nats.subscribe("foo.>") { |msg, reply, subject| puts "Received on '#{subject}': '#{msg}'" }

# Simple Publisher
nats.publish('foo.bar.baz', 'Hello World!')

# Unsubscribing
sid = nats.subscribe('bar') { |msg| puts "Received : '#{msg}'" }
nats.unsubscribe(sid)

# Requests
nats.request('help') { |response| puts "Got a response: '#{response}'" }

# Replies
nats.subscribe('help') do |msg, reply, subject|
  puts "Received on '#{subject}': '#{msg}'"
  nats.publish(reply, "I'll help!")
end

# Request with timeout
begin
  msg = nats.timed_request('help', 'please', 0.5)
  puts "Received on '#{msg[:subject]}': #{msg[:data]}"
rescue NATS::IO::Timeout
  puts "nats: request timed out"
end

# Server roundtrip which fails if it does not happen within 500ms
begin
  nats.flush(0.5)
rescue NATS::IO::Timeout
  puts "nats: flush timeout"
end

# Closes connection to NATS
nats.close
