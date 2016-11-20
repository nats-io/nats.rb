require 'nats/io/client'

nats = NATS::IO::Client.new

nats.connect(servers: ["nats://127.0.0.1:4222"])
puts "Connected to #{nats.connected_server}"

# Simple subscriber
nats.subscribe("foo.>") { |msg, reply, subject| puts "[Received] on '#{subject}': '#{msg}'" }

# Simple Publisher
nats.publish('foo.bar.baz', 'Hello World!')

# Unsubscribing
sid = nats.subscribe('bar') { |msg| puts "Received : '#{msg}'" }
nats.unsubscribe(sid)

# Subscribers which reply to requests
nats.subscribe('help') do |msg, reply, subject|
  puts "[Received] on '#{subject}' #{reply}: '#{msg}'"
  nats.publish(reply, "I'll help!") if reply
end

nats.subscribe('>') do |msg, reply, subject|
  puts "[Received] via wildcard on '#{subject}' #{reply}: '#{msg}'"
  nats.publish(reply, "Hi") if reply
end

# Requests happens asynchronously if given a callback
nats.request('help', 'world', max: 2) do |response|
  puts "[Response] '#{response}'"
end

# Request without a callback waits for the response or times out.
begin
  msg = nats.request('help', 'please', timeout: 1.0)
  puts "[Response] '#{msg.subject}': #{msg.data}"
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
