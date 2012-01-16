
require 'optparse'

$:.unshift File.expand_path('../../lib', __FILE__)
require 'nats/client'

$expected = 100000
$hash = 2500
$sub  = 'test'
$qs = 5
$qgroup = 'mycoolgroup'

STDOUT.sync = true

parser = OptionParser.new do |opts|
  opts.banner = "Usage: queues_perf [options]"

  opts.separator ""
  opts.separator "options:"

  opts.on("-n ITERATIONS", "iterations to expect (default: #{$expected})") { |iter| $expected = iter.to_i }
  opts.on("-s SUBJECT", "Send subject (default: #{$sub})")                 { |nsub| $sub = nsub }
  opts.on("-q QUEUE SUBSCRIBERS", "# subscribers (default: #{$qs})")        { |qs| $qs = qs }
end

parser.parse(ARGV)

trap("TERM") { exit! }
trap("INT")  { exit! }

NATS.on_error { |err| puts "Server Error: #{err}"; exit! }

NATS.start do
  received = 1
  (0...$qs).each do
    NATS.subscribe($sub, :queue => $qgroup) do
      ($start = Time.now and puts "Started Receiving..") if (received == 1)
      if ((received+=1) == $expected)
        puts "\nTest completed : #{($expected/(Time.now-$start)).ceil} msgs/sec.\n"
        NATS.stop
      end
      printf('+') if received.modulo($hash) == 0
    end
  end
  puts "Waiting for #{$expected} messages on [#{$sub}] on #{$qs} queue receivers on group: [#{$qgroup}]"
end
