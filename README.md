# NATS

EventMachine based Publish-Subscribe Messaging that just works.

## Supported Platforms

This gem currently works on the following Ruby platforms:

- MRI 1.8 and 1.9 (Performance is best on 1.9.2)
- Rubinius
- JRuby (should work)

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

      # Unsubscribing
      s1 = nc.subscribe('bar') { |sub, msg| puts "Msg received on [#{sub}] : '#{msg}' }
      nc.unsubscribe(s1)

      # Stop using NATS.stop
      EM.next_tick { NATS.stop }

    end

See examples and benchmark for more..

## License

(The MIT License)

Copyright (c) 2009,2010 Derek Collison

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

