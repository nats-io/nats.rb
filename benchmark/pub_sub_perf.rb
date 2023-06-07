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
require 'concurrent'
require 'ruby-progressbar'

$:.unshift File.expand_path('../../lib', __FILE__)
require 'nats/io/client'

$count = 100000
$batch = 100

$subscriptions = 1
$concurrency = 1

$delay = 0.00001

TSIZE = 4*1024

$sub  = 'test'
$data_size = 16

$stdout.sync = true

parser = OptionParser.new do |opts|
  opts.banner = "Usage: pub_perf [options]"

  opts.separator ""
  opts.separator "options:"

  opts.on("-n COUNT",   "Messages to send (default: #{$count}}") { |count| $count = count.to_i }
  opts.on("-s SIZE",    "Message size (default: #{$data_size})") { |size| $data_size = size.to_i }
  opts.on("-S SUBJECT", "Send subject (default: (#{$sub})")      { |sub| $sub = sub }
  opts.on("-b BATCH",   "Batch size (default: (#{$batch})")      { |batch| $batch = batch.to_i }
  opts.on("-c SUBSCRIPTIONS", "Subscription number (default: (#{$subscriptions})") { |subscriptions| $subscriptions = subscriptions.to_i }
  opts.on("-t CONCURRENCY", "Subscription processing concurrency (default: (#{$concurrency})") { |concurrency| $concurrency = concurrency.to_i }
end

parser.parse(ARGV)

$data = Array.new($data_size) { "%01x" % rand(16) }.join('').freeze

puts "Sending #{$count} messages of size #{$data.size} bytes on [#{$sub}], receiving each in #{$subscriptions} subscriptions"

$progressbar = ProgressBar.create(title: "Received", total: $count*$subscriptions, format: '%t: |%B| %p%% %a %e', autofinish: false, throttle_rate: 0.1)

def results
  elapsed  = Time.now - $start
  mbytes = sprintf("%.1f", (($data_size*$received)/elapsed)/(1024*1024))
  <<~MSG

    Test completed: #{($received/elapsed).ceil} received msgs/sec (#{mbytes} MB/sec)
    Received #{$received} messages in #{elapsed} seconds
  MSG
end

trap("TERM") { puts results; exit! }
trap("INT")  { puts results; exit! }

nats = NATS::IO::Client.new
nats.connect

$batch = 10 if $data_size >= TSIZE

$received = 0

$subscriptions.times do
  subscription = nats.subscribe($sub) { $received += 1; $progressbar.progress = $received }
  subscription.processing_concurrency = $concurrency
end

$start   = Time.now
$to_send = $count

loop do
  (0..$batch).each do
    $to_send -= 1
    nats.publish($sub, $data)

    break if $to_send.zero?

    sleep $delay if $to_send.modulo(1000) == 0
  end
  break if $to_send.zero?
end

# Finish and let client to process everything
finished = Concurrent::Event.new
nats.on_close { finished.set }
nats.flush(300)
nats.drain
finished.wait
$progressbar.finish

puts results
