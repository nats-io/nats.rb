# Copyright 2016-2018 The NATS Authors
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

require 'optparse'

$LOAD_PATH << File.expand_path('../../lib', __FILE__)
require 'nats/io/client'

$loop = 10000
$hash = 250
$sub  = 'test'

$subscriptions = 1
$concurrency = 1
$queue = nil

$stdout.sync = true

parser = OptionParser.new do |opts|
  opts.banner = "Usage: latency_perf [options]"

  opts.separator ""
  opts.separator "options:"

  opts.on("-s SUBJECT", "Send subject (default: #{$sub})")             { |sub| $sub = sub }
  opts.on("-n ITERATIONS", "iterations to expect (default: #{$loop})") { |iter| $loop = iter.to_i }
  opts.on("-c SUBSCRIPTIONS", "Subscription number (default: (#{$subscriptions})") { |subscriptions| $subscriptions = subscriptions.to_i }
  opts.on("-t CONCURRENCY", "Subscription processing concurrency (default: (#{$concurrency})") { |concurrency| $concurrency = concurrency.to_i }
  opts.on("-q QUEUE", "Queue Subscription group") { |queue| $queue = queue }
end

parser.parse(ARGV)
$drain = $loop

trap("TERM") { exit! }
trap("INT")  { exit! }

nats = NATS::IO::Client.new

nats.on_error do |e|
  puts "nats: error: #{e}"
end
nats.on_close do
  puts "nats: connection closed"
end
nats.on_disconnect do
  puts "nats: disconnected!"
end
nats.on_reconnect do
  puts "nats: reconnected!"
end

nats.connect(:max_reconnect => 10)

if $queue
  $subscriptions.times do
    sub = nats.subscribe($sub, queue: $queue) do |msg|
      msg.respond("OK:"+msg.data)
    end
    sub.processing_concurrency = $concurrency
  end
else
  $subscriptions.times do
    sub = nats.subscribe($sub) do |msg|
      msg.respond("OKOK:"+msg.data)
    end
    sub.processing_concurrency = $concurrency
  end
end
nats.flush(5)

timeouts = 0
puts "Sending #{$loop} request/responses"
$start = Time.now

loop do
  begin
    nats.request($sub, "AAA-#{$drain}", timeout: 2)
  rescue NATS::IO::Timeout => e
    timeouts += 1
  end

  $drain-=1
  if $drain == 0
    ms = "%.2f" % (((Time.now-$start)/$loop)*1000.0)
    puts "\nTest completed : #{ms} ms avg request/response latency\n"
    puts "Timeouts: #{timeouts}" if timeouts > 0
    exit!
  else
    printf('#') if $drain.modulo($hash) == 0
  end
end
