#!/usr/bin/env ruby

require 'rubygems'
require 'nats/client'

trap("TERM") { NATS.stop }
trap("INT")  { NATS.stop }

subject = ARGV.shift
usage unless subject

NATS.on_error { |err| puts "Server Error: #{err}"; exit! }

NATS.start do
  puts "Listening on [#{subject}]"
  NATS.subscribe(subject) { |msg| puts "Received '#{msg}'" }
end
