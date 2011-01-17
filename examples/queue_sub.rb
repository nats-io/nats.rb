#!/usr/bin/env ruby

require 'rubygems'
require 'nats/client'

trap("TERM") { NATS.stop }
trap("INT")  { NATS.stop }

def usage
  puts "Usage: ruby queue_sub.rb <subject> <queue name>"; exit
end

subject, queue_group = ARGV
usage unless subject and queue_group

NATS.on_error { |err| puts "Server Error: #{err}"; exit! }

NATS.start do
  puts "Listening on [#{subject}], queue group [#{queue_group}]"
  NATS.subscribe(subject, queue_group) { |msg| puts "Received '#{msg}'" }
end
