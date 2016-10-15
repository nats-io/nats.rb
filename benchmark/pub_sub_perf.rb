
require 'optparse'

$:.unshift File.expand_path('../../lib', __FILE__)
require 'nats/io/client'

$count = 100000
$batch = 100

$delay = 0.00001
$dmin  = 0.00001

TRIP  = (2*1024*1024)
TSIZE = 4*1024

$sub  = 'test'
$data_size = 16

$hash  = 2500

$stdout.sync = true

parser = OptionParser.new do |opts|
  opts.banner = "Usage: pub_perf [options]"

  opts.separator ""
  opts.separator "options:"

  opts.on("-n COUNT",   "Messages to send (default: #{$count}}") { |count| $count = count.to_i }
  opts.on("-s SIZE",    "Message size (default: #{$data_size})") { |size| $data_size = size.to_i }
  opts.on("-S SUBJECT", "Send subject (default: (#{$sub})")      { |sub| $sub = sub }
  opts.on("-b BATCH",   "Batch size (default: (#{$batch})")      { |batch| $batch = batch.to_i }
end

parser.parse(ARGV)

trap("TERM") { exit! }
trap("INT")  { exit! }

$data = Array.new($data_size) { "%01x" % rand(16) }.join('').freeze

nats = NATS::IO::Client.new
nats.connect

$batch = 10 if $data_size >= TSIZE

$received = 0
nats.subscribe($sub) { $received += 1; print('+') if $received.modulo($hash) == 0 }

$start   = Time.now
$to_send = $count

puts "Sending #{$count} messages of size #{$data.size} bytes on [#{$sub}]"

loop do
  (0..$batch).each do
    $to_send -= 1
    if $to_send == 0
      nats.publish($sub, $data)
      nats.flush

      elapsed = Time.now - $start
      mbytes = sprintf("%.1f", (($data_size*$count)/elapsed)/(1024*1024))
      puts "\nTest completed : #{($count/elapsed).ceil} sent/received msgs/sec (#{mbytes} MB/sec)\n"
      puts "Received #{$received} messages\n"
      exit
    else
      nats.publish($sub, $data)
    end
    sleep $delay if $to_send.modulo(1000) == 0
    print('#') if $to_send.modulo($hash) == 0
  end
end
