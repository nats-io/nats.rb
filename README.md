# NATS - Ruby Client

A [Ruby](http://ruby-lang.org) client for the [NATS messaging system](https://nats.io).

[![License Apache 2.0](https://img.shields.io/badge/License-Apache2-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![Build Status](https://travis-ci.org/nats-io/nats.rb.svg)](http://travis-ci.org/nats-io/nats.rb) [![Gem Version](https://d25lcipzij17d.cloudfront.net/badge.svg?id=rb&type=5&v=0.11.0)](https://rubygems.org/gems/nats/versions/0.11.0) [![Yard Docs](http://img.shields.io/badge/yard-docs-blue.svg)](https://www.rubydoc.info/gems/nats)

## Getting Started

```bash
gem install nats

nats-sub foo &
nats-pub foo 'Hello World!'
```

Starting from [v0.11.0](https://github.com/nats-io/nats.rb/releases/tag/v0.11.0) release,
you can also optionally install [NKEYS](https://github.com/nats-io/nkeys.rb) in order to use
the new NATS v2.0 auth features:

```bash
gem install nkeys
```

If you're looking for a non-EventMachine alternative, check out the [nats-pure](https://github.com/nats-io/nats-pure.rb) gem.

## Basic Usage

```ruby
require "nats/client"

NATS.start do

  # Simple Subscriber
  NATS.subscribe('foo') { |msg| puts "Msg received : '#{msg}'" }

  # Simple Publisher
  NATS.publish('foo.bar.baz', 'Hello World!')

  # Unsubscribing
  sid = NATS.subscribe('bar') { |msg| puts "Msg received : '#{msg}'" }
  NATS.unsubscribe(sid)

  # Requests
  NATS.request('help') { |response| puts "Got a response: '#{response}'" }

  # Replies
  NATS.subscribe('help') { |msg, reply| NATS.publish(reply, "I'll help!") }

  # Stop using NATS.stop, exits EM loop if NATS.start started the loop
  NATS.stop

end
```

## Wildcard Subscriptions

```ruby
# "*" matches any token, at any level of the subject.
NATS.subscribe('foo.*.baz') { |msg, reply, sub| puts "Msg received on [#{sub}] : '#{msg}'" }
NATS.subscribe('foo.bar.*') { |msg, reply, sub| puts "Msg received on [#{sub}] : '#{msg}'" }
NATS.subscribe('*.bar.*')   { |msg, reply, sub| puts "Msg received on [#{sub}] : '#{msg}'" }

# ">" matches any length of the tail of a subject and can only be the last token
# E.g. 'foo.>' will match 'foo.bar', 'foo.bar.baz', 'foo.foo.bar.bax.22'
NATS.subscribe('foo.>') { |msg, reply, sub| puts "Msg received on [#{sub}] : '#{msg}'" }
```

## Queues Groups

```ruby
# All subscriptions with the same queue name will form a queue group
# Each message will be delivered to only one subscriber per queue group, queuing semantics
# You can have as many queue groups as you wish
# Normal subscribers will continue to work as expected.
NATS.subscribe(subject, :queue => 'job.workers') { |msg| puts "Received '#{msg}'" }
```

## Clustered Usage

```ruby
NATS.start(:servers => ['nats://127.0.0.1:4222', 'nats://127.0.0.1:4223']) do |nc|
  puts "NATS is connected to #{nc.connected_server}"

  nc.on_reconnect do
    puts "Reconnected to server at #{nc.connected_server}"
  end

  nc.on_disconnect do |reason|
    puts "Disconnected: #{reason}"
  end

  nc.on_close do
    puts "Connection to NATS closed"
  end
end

opts = {
  :dont_randomize_servers => true,
  :reconnect_time_wait => 0.5,
  :max_reconnect_attempts => 10,
  :servers => ['nats://127.0.0.1:4222', 'nats://127.0.0.1:4223', 'nats://127.0.0.1:4224']
}

NATS.connect(opts) do |c|
  puts "NATS is connected!"
end
```

### Auto discovery

The client also auto discovers new nodes announced by the server as
they attach to the cluster.  Reconnection logic parameters such as
time to back-off on failure and max attempts apply the same to both
discovered nodes and those defined explicitly on connect:

```ruby
opts = {
  :dont_randomize_servers => true,
  :reconnect_time_wait => 0.5,
  :max_reconnect_attempts => 10,
  :servers => ['nats://127.0.0.1:4222', 'nats://127.0.0.1:4223'],
  :user => 'secret',
  :pass => 'deadbeef'
}

NATS.connect(opts) do |c|
  # Confirm number of available servers in cluster.
  puts "Connected to NATS! Servers in pool: #{c.server_pool.count}"
end
```

## Advanced Usage

```ruby
# Publish with closure, callback fires when server has processed the message
NATS.publish('foo', 'You done?') { puts 'msg processed!' }

# Timeouts for subscriptions
sid = NATS.subscribe('foo') { received += 1 }
NATS.timeout(sid, TIMEOUT_IN_SECS) { timeout_recvd = true }

# Timeout unless a certain number of messages have been received
NATS.timeout(sid, TIMEOUT_IN_SECS, :expected => 2) { timeout_recvd = true }

# Auto-unsubscribe after MAX_WANTED messages received
NATS.unsubscribe(sid, MAX_WANTED)

# Multiple connections
NATS.subscribe('test') do |msg|
  puts "received msg"
  
  # Gracefully disconnect from NATS after handling
  # messages that have already been delivered by server.
  NATS.drain
end

# Form second connection to send message on
NATS.connect { NATS.publish('test', 'Hello World!') }
```

See examples and benchmarks for more information..

### TLS

Advanced customizations options for setting up a secure connection can
be done by including them on connect:

```ruby
options = {
  :servers => [
   'nats://secret:deadbeef@127.0.0.1:4443',
   'nats://secret:deadbeef@127.0.0.1:4444'
  ],
  :max_reconnect_attempts => 10,
  :reconnect_time_wait => 2,
  :tls => {
    :private_key_file => './spec/configs/certs/key.pem',
    :cert_chain_file  => './spec/configs/certs/server.pem'
    # Can enable verify_peer functionality optionally by passing
    # the location of a ca_file.
    # :verify_peer => true,
    # :ca_file => './spec/configs/certs/ca.pem'
  }
}

# Set default callbacks
NATS.on_error do |e|
  puts "Error: #{e}"
end

NATS.on_disconnect do |reason|
  puts "Disconnected: #{reason}"
end

NATS.on_reconnect do |nats|
  puts "Reconnected to NATS server at #{nats.connected_server}"
end

NATS.on_close do
  puts "Connection to NATS closed"
  EM.stop
end

NATS.start(options) do |nats|
  puts "Connected to NATS at #{nats.connected_server}"

  nats.subscribe("hello") do |msg|
    puts "Received: #{msg}"
  end

  nats.flush do
    nats.publish("hello", "world")
  end
end
```

### Fibers

Requests without a callback can be made to work synchronously and return 
the result when running in a Fiber.  For these type of requests, it is
possible to set a timeout of how long to wait for a single or multiple
responses.

```ruby
NATS.start {

  NATS.subscribe('help') do |msg, reply|
    puts "[Received]: <<- #{msg}"
    NATS.publish(reply, "I'll help! - #{msg}")
  end

  NATS.subscribe('slow') do |msg, reply|
    puts "[Received]: <<- #{msg}"
    EM.add_timer(1) { NATS.publish(reply, "I'll help! - #{msg}") }
  end

  10.times do |n|
    NATS.subscribe('hi') do |msg, reply|
      NATS.publish(reply, "Hello World! - id:#{n}")
    end
  end

  Fiber.new do
    # Requests work synchronously within the same Fiber
    # returning the message when done.
    response = NATS.request('help', 'foo')
    puts "[Response]: ->> '#{response}'"

    # Specifying a custom timeout to give up waiting for
    # a response.
    response = NATS.request('slow', 'bar', timeout: 2)
    if response.nil?
      puts "No response after 2 seconds..."
    else
      puts "[Response]: ->> '#{response}'"
    end

    # Can gather multiple responses with the same request
    # which will then return a collection with the responses
    # that were received before the timeout.
    responses = NATS.request('hi', 'quux', max: 10, timeout: 1)
    responses.each_with_index do |response, i|
      puts "[Response# #{i}]: ->> '#{response}'"
    end
    
    # If no replies then an empty collection is returned.
    responses = NATS.request('nowhere', '', max: 10, timeout: 2)
    if responses.any?
      puts "Got #{responses.count} responses"
    else
      puts "No response after 2 seconds..."
    end

    NATS.stop
  end.resume

  # Multiple fibers can make requests concurrently
  # under the same Eventmachine loop.
  Fiber.new do
    10.times do |n|
      response = NATS.request('help', "help.#{n}")
      puts "[Response]: ->> '#{response}'"
    end
  end.resume
}
```

### New Authentication (Nkeys and User Credentials)

This requires server with version >= 2.0.0

NATS servers have a new security and authentication mechanism to authenticate with user credentials and NKEYS. A single file containing the JWT and NKEYS to authenticate against a NATS v2 server can be set with the `user_credentials` option:

```ruby
require 'nats/client'

NATS.start("tls://connect.ngs.global", user_credentials: "/path/to/creds") do |nc|
  nc.subscribe("hello") do |msg|
    puts "[Received] #{msg}"
  end
  nc.publish('hello', 'world')
end
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
require 'nats/client'

NATS.start("tls://connect.ngs.global", nkeys_seed: "path/to/seed.txt") do |nc|
  nc.subscribe("hello") do |msg|
    puts "[Received] #{msg}"
  end
  nc.publish('hello', 'world')
end

```

## License

Unless otherwise noted, the NATS source files are distributed under
the Apache Version 2.0 license found in the LICENSE file.
