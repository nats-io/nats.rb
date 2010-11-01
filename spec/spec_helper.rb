
require './lib/nats/client'
require 'pp'

def timeout_nats_on_failure(to=0.1)
  EM.add_timer(to) { NATS.stop }
end
