# NATS

A simple publish-subscribe messaging system.

## Supported Platforms

This gem currently works on the following Ruby platforms:

- MRI 1.8 and 1.9 (Performance is best on 1.9.2)
- Rubinius

## Usage

    require "nats/client"

    NATS.start do |nc|
      # Simple Subscriber
      nc.subscribe('foo') { |sub, msg| puts "Msg received on [#{sub}] : '#{msg}' }

      # Wildcards

      # '*" matches any token
      nc.subscribe('foo.*.baz') { |sub, msg| puts "Msg received on [#{sub}] : '#{msg}' }

      # '>" can only be last token, and matches to any depth
      nc.subscribe('foo.>') { |sub, msg| puts "Msg received on [#{sub}] : '#{msg}' }

      # Simple Publisher
      nc.publish('foo.bar.baz', 'Hello World!')
    end

See under examples and benchmark for more..