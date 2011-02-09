# NATS

A lightweight EventMachine based publish-subscribe messaging system.

## Supported Platforms

This gem currently works on the following Ruby platforms:

- MRI 1.8 and 1.9 (Performance is best on 1.9.2)
- Rubinius
- JRuby

## Getting Started

    [sudo] gem install nats
     == or ==
    [sudo] rake geminstall

    nats-sub foo &
    nats-pub foo "Hello World!'

## Usage

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

      # Stop using NATS.stop, exits EM loop if NATS.start started the loop.
      NATS.stop

    end

## Wildcard Subscriptions

      # '*" matches any token
      NATS.subscribe('foo.*.baz') { |msg, reply, sub| puts "Msg received on [#{sub}] : '#{msg}'" }

      # '>" can only be last token, and matches to any depth
      NATS.subscribe('foo.>') { |msg, reply, sub| puts "Msg received on [#{sub}] : '#{msg}'" }

## Advanced Usage

      # Publish with closure, callback fires when server has processed the message
      NATS.publish('foo', 'You done?') { puts 'msg processed!' }

      # Sending replies
      NATS.subscribe('help') do |msg, reply|
        NATS.publish(reply, "I'll help!")
      end

      # Timeouts for subscriptions
      sid = NATS.subscribe('foo') { received += 1 }
      NATS.timeout(sid, TIMEOUT_IN_SECS) { timeout_recvd = true }

      # Timeout unless a certain number of messages have been received
      NATS.timeout(sid, TIMEOUT_IN_SECS, :expected => 2) { timeout_recvd = true }

      # Auto-unsunscribe after MAX_WANTED messages received
      NATS.unsubscribe(sid, MAX_WANTED)

See examples and benchmark for more information..

## License

(The MIT License)

Copyright (c) 2010, 2011 Derek Collison

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

