require 'rubygems'
require 'nats/client'

# This is an example to show off nats-top. Run busy_body on a monitor enabled
# server and exec nats-top.

['TERM', 'INT'].each { |sig| trap(sig) { exit! } }

NATS.on_error { |err| puts "Server Error: #{err}"; exit! }

def create_subscribers(sub='foo.bar', num_subs=10, num_connections=20)
  (1..num_connections).each do
    NATS.connect do |nc|
      (1..num_subs).each { nc.subscribe(sub) }
    end
  end
end

def create_publishers(sub='foo.bar', body='Hello World!', num_connections=20, num_sends=100)
  (1..num_connections).each do
    NATS.connect do |nc|
      (1..num_sends).each { nc.publish(sub, body) }
    end
  end
end

def timed_publish(sub='foo.bar', body='Hello World!', delay=1, burst=500)
  EM.add_periodic_timer(1) do
    b = (burst * rand).to_i
    (1..b).each { NATS.publish(sub, body) }
  end
end

NATS.start {
  create_subscribers
  create_publishers
  timed_publish
}
