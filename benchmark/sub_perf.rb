
require 'optparse'
require File.dirname(__FILE__) + '/../lib/nats/client'

$expected = 100000
$hash = 10000
$sub  = 'test'

STDOUT.sync = true

parser = OptionParser.new do |opts|
  opts.banner = "Usage: sub_perf [options]"

  opts.separator ""
  opts.separator "options:"

  opts.on("-n ITERATIONS", "iterations to expect (default: #{$expected}")    { |iter| $expected = iter.to_i }
  opts.on("-s SUBJECT", "Send subject (default: #{$sub})")                   { |nsub| $sub = nsub }
end

parser.parse(ARGV)

trap("TERM") { exit! }
trap("INT")  { exit! }

NATS.on_error { |err| puts "Server Error: #{err}"; exit! }

NATS.start do

  received = 1
  NATS.subscribe($sub) {
    ($start = Time.now and puts "Started Receiving..") if (received == 1)
    if ((received+=1) == $expected)
      puts "\nTest completed : #{($expected/(Time.now-$start)).ceil} msgs/sec.\n"
      NATS.stop
    end
    printf('+') if received.modulo($hash) == 0
  }

  puts "Waiting for #{$expected} messages on [#{$sub}]"
end
