# NATS - Pure Ruby Client

**Status**: _work in progress_

A thread safe [Ruby](http://ruby-lang.org) client for the [NATS messaging system](https://nats.io) written in pure Ruby with no dependencies.

[![License MIT](https://img.shields.io/npm/l/express.svg)](http://opensource.org/licenses/MIT)[![Build Status](https://travis-ci.org/wallyqs/pure-ruby-nats.svg)](http://travis-ci.org/wallyqs/pure-ruby-nats)

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

# Requests
nats.request('help', 'please') { |response| puts "Got a response: '#{response}'" }

# Replies
nats.subscribe('help') do |msg, reply, subject|
  puts "Received on '#{subject}': '#{msg}'"
  nats.publish(reply, "I'll help!")
end

# Request with timeout
begin
  msg = nats.request('help', 'please', timeout: 0.5)
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
```

## Clustered Usage

```ruby
require 'nats/io/client'

nats = NATS::IO::Client.new

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

(The MIT License)

Copyright (c) 2016 Apcera

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to
deal in the Software without restriction, including without limitation the
rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
sell copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
IN THE SOFTWARE.
