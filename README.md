# NATS - Pure Ruby Client

A thread safe [Ruby](http://ruby-lang.org) client for the [NATS messaging system](https://nats.io) written in pure Ruby.

[![License Apache 2.0](https://img.shields.io/badge/License-Apache2-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)[![Build Status](https://travis-ci.org/nats-io/nats-pure.rb.svg)](http://travis-ci.org/nats-io/nats-pure.rb)[![Gem Version](https://d25lcipzij17d.cloudfront.net/badge.svg?id=rb&type=5&v=0.5.0)](https://rubygems.org/gems/nats-pure/versions/0.5.0)

## Getting Started

```bash
gem install nats-pure
```

## Basic Usage

```ruby
require 'nats/io/client'

nats = NATS::IO::Client.new

nats.connect(:servers => ["nats://127.0.0.1:4222"])
puts "Connected to #{nats.connected_server}"

# Simple subscriber
nats.subscribe("foo.>") { |msg, reply, subject| puts "Received on '#{subject}': '#{msg}'" }

# Simple Publisher
nats.publish('foo.bar.baz', 'Hello World!')

# Unsubscribing
sid = nats.subscribe('bar') { |msg| puts "Received : '#{msg}'" }
nats.unsubscribe(sid)

# Requests with a block handles replies asynchronously
nats.request('help', 'please', max: 5) { |response| puts "Got a response: '#{response}'" }

# Replies
nats.subscribe('help') do |msg, reply, subject|
  puts "Received on '#{subject}': '#{msg}'"
  nats.publish(reply, "I'll help!")
end

# Request without a block waits for response or timeout
begin
  msg = nats.request('help', 'please', timeout: 0.5)
  puts "Received on '#{msg.subject}': #{msg.data}"
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
```

## Clustered Usage

```ruby
require 'nats/io/client'

nats = NATS::IO::Client.new

nats.on_error do |e|
  puts "Error: #{e}"
end

nats.on_reconnect do
  puts "Reconnected to server at #{nats.connected_server}"
end

nats.on_disconnect do
  puts "Disconnected!"
end

nats.on_close do
  puts "Connection to NATS closed"
end

cluster_opts = {
  servers: ["nats://127.0.0.1:4222", "nats://127.0.0.1:4223"],
  dont_randomize_servers: true,
  reconnect_time_wait: 0.5,
  max_reconnect_attempts: 2
}

nats.connect(cluster_opts)
puts "Connected to #{nats.connected_server}"

nats.subscribe("hello") do |data|
  puts "#{Time.now} - Received: #{data}"
end

n = 0
loop do
  n += 1
  nats.publish("hello", "world.#{n}")
  sleep 0.1
end
```

## TLS

It is possible to setup a custom TLS connection to NATS by passing
an [OpenSSL](http://ruby-doc.org/stdlib-2.3.2/libdoc/openssl/rdoc/OpenSSL/SSL/SSLContext.html) context to the client to be used on connect:

```ruby
tls_context = OpenSSL::SSL::SSLContext.new
tls_context.ssl_version = :TLSv1_2

nats.connect({
   servers: ['tls://127.0.0.1:4444'],
   reconnect: false,
   tls: {
     context: tls_context
   }
 })
```

## License

Unless otherwise noted, the NATS source files are distributed under
the Apache Version 2.0 license found in the LICENSE file.
