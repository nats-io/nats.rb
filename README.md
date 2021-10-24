# NATS - Pure Ruby Client

A thread safe [Ruby](http://ruby-lang.org) client for the [NATS messaging system](https://nats.io) written in pure Ruby.

[![License Apache 2.0](https://img.shields.io/badge/License-Apache2-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)[![Build Status](https://travis-ci.org/nats-io/nats-pure.rb.svg)](http://travis-ci.org/nats-io/nats-pure.rb)[![Gem Version](https://d25lcipzij17d.cloudfront.net/badge.svg?id=rb&type=5&v=0.7.0)](https://rubygems.org/gems/nats-pure/versions/0.7.0)

## Getting Started

```bash
gem install nats-pure
```

Starting from [v0.6.0](https://github.com/nats-io/nats-pure.rb/releases/tag/v0.6.0) release,
you can also optionally install [NKEYS](https://github.com/nats-io/nkeys.rb) in order to use
the new NATS v2.0 auth features:

```bash
gem install nkeys
```

## Basic Usage

```ruby
require 'nats/io/client'

nats = NATS.connect("demo.nats.io")
puts "Connected to #{nats.connected_server}"

# Simple subscriber
nats.subscribe("foo.>") { |msg, reply, subject| puts "Received on '#{subject}': '#{msg}'" }

# Simple Publisher
nats.publish('foo.bar.baz', 'Hello World!')

# Unsubscribing
sub = nats.subscribe('bar') { |msg| puts "Received : '#{msg}'" }
sub.unsubscribe()

# Requests with a block handles replies asynchronously
nats.request('help', 'please', max: 5) { |response| puts "Got a response: '#{response}'" }

# Replies
sub = nats.subscribe('help') do |msg|
  puts "Received on '#{msg.subject}': '#{msg.data}' with headers: #{msg.header}"
  msg.respond("I'll help!")
end

# Request without a block waits for response or timeout
begin
  msg = nats.request('help', 'please', timeout: 0.5)
  puts "Received on '#{msg.subject}': #{msg.data}"
rescue NATS::IO::Timeout
  puts "nats: request timed out"
end

# Request using a message with headers
begin
  msg = NATS::Msg.new(subject: "help", headers: {foo: 'bar'})
  resp = nats.request_msg(msg)
  puts "Received on '#{resp.subject}': #{resp.data}"
rescue NATS::IO::Timeout => e
  puts "nats: request timed out: #{e}"
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

nats = NATS.connect

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

NATS.connect(cluster_opts)
puts "Connected to #{nats.connected_server}"

nats.subscribe("hello") do |msg|
  puts "#{Time.now} - Received: #{msg.data}"
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

NATS.connect({
   servers: ['tls://127.0.0.1:4444'],
   reconnect: false,
   tls: {
     context: tls_context
   }
 })
```

### New Authentication (Nkeys and User Credentials)

This requires server with version >= 2.0.0

NATS servers have a new security and authentication mechanism to authenticate with user credentials and NKEYS. A single file containing the JWT and NKEYS to authenticate against a NATS v2 server can be set with the `user_credentials` option:

```ruby
NATS.connect("tls://connect.ngs.global", user_credentials: "/path/to/creds")
```

This will create two callback handlers to present the user JWT and sign the nonce challenge from the server. The core client library never has direct access to your private key and simply performs the callback for signing the server challenge. The library will load and wipe and clear the objects it uses for each connect or reconnect.

Bare NKEYS are also supported. The nkey seed should be in a read only file, e.g. `seed.txt`.

```bash
> cat seed.txt
# This is my seed nkey!
SUAGMJH5XLGZKQQWAWKRZJIGMOU4HPFUYLXJMXOO5NLFEO2OOQJ5LPRDPM
```

Then in the client specify the path to the seed using the `nkeys_seed` option:

```ruby
NATS.connect("tls://connect.ngs.global", nkeys_seed: "path/to/seed.txt")
```

## License

Unless otherwise noted, the NATS source files are distributed under
the Apache Version 2.0 license found in the LICENSE file.
