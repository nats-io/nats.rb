
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
