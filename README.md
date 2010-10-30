# NATS

A simple publish-subscribe messaging system.

## Supported Platforms

This gem currently works on the following Ruby platforms :

- MRI 1.8 and 1.9
- Rubinius

## Usage

    require "nats/client"

    NATS.start do |nc|
      # Simple Subscriber
      nc.subscribe('foo') { |sub, msg| puts "Msg received on [#{sub}] : '#{msg}' }

      # Simple Publisher
      nc.publish('foo', 'Hello World!')
    end
