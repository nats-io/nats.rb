
require './lib/nats/client'
require 'pp'

def timeout_nats_on_failure(to=0.25)
  EM.add_timer(to) { NATS.stop }
end
