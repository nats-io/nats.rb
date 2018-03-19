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

$stdout.sync = true

parser = OptionParser.new do |opts|
  opts.banner = "Usage: latency_perf [options]"

  opts.separator ""
  opts.separator "options:"

  opts.on("-s SUBJECT", "Send subject (default: #{$sub})")             { |sub| $sub = sub }
  opts.on("-n ITERATIONS", "iterations to expect (default: #{$loop})") { |iter| $loop = iter.to_i }
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

nats.subscribe($sub) do |msg, reply|
  nats.publish(reply)
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
