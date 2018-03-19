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
require 'monitor'

$:.unshift File.expand_path('../../lib', __FILE__)
require 'nats/io/client'

$expected = 100000
$hash = 2500
$sub  = 'test'

$stdout.sync = true

parser = OptionParser.new do |opts|
  opts.banner = "Usage: sub_perf [options]"

  opts.separator ""
  opts.separator "options:"

  opts.on("-n COUNT", "Messages to expect (default: #{$expected})") { |count| $expected = count.to_i }
  opts.on("-s SUBJECT", "Send subject (default: #{$sub})")          { |sub| $sub = sub }
end

parser.parse(ARGV)

trap("TERM") { exit! }
trap("INT")  { exit! }

nats = NATS::IO::Client.new

nats.connect
done = nats.new_cond
received = 1
nats.subscribe($sub) do
  ($start = Time.now and puts "Started Receiving!") if (received == 1)
  if ((received += 1) == $expected)
    puts "\nTest completed : #{($expected/(Time.now-$start)).ceil} msgs/sec.\n"
    nats.synchronize do
      done.signal
    end
  end
  printf('+') if received.modulo($hash) == 0
end

puts "Waiting for #{$expected} messages on [#{$sub}]"
nats.synchronize do
  done.wait
end
