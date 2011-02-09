
require 'optparse'

$:.unshift File.expand_path('../../lib', __FILE__)
require 'nats/client'

$count = 100000
$batch = 10

$sub  = 'test'
$data_size = 128

$hash  = 2500

STDOUT.sync = true

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

NATS.on_error { |err| puts "Server Error: #{err}"; exit! }

$data = Array.new($data_size) { "%01x" % rand(16) }.join('').freeze

NATS.start do

  $start   = Time.now
  $to_send = $count

  def send_batch
    (0..$batch).each do
      $to_send -= 1
      if $to_send == 0
        NATS.publish($sub, $data) { display_final_results }
        return
      else
        NATS.publish($sub, $data)
      end
      printf('+') if $to_send.modulo($hash) == 0
    end
    EM.next_tick { send_batch }
  end

  def display_final_results
    elapsed = Time.now - $start
    mbytes = sprintf("%.1f", (($data_size*$count)/elapsed)/(1024*1024))
    puts "\nTest completed : #{($count/elapsed).ceil} msgs/sec (#{mbytes} MB/sec)\n"
    NATS.stop
  end

  if false
    EM.add_periodic_timer(0.25) do
      puts "Outstanding data size is #{NATS.client.get_outbound_data_size}"
    end
  end

  puts "Sending #{$count} messages of size #{$data.size} bytes on [#{$sub}]"

  # kick things off..
  send_batch

end
