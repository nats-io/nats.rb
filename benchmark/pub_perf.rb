
require 'optparse'
require File.dirname(__FILE__) + '/../lib/nats/client'

$loop = 100000
$hash = 10000

$max_outstanding = (512*1024) #512k

$data = '-ok-'
$sub  = 'test'

STDOUT.sync = true

parser = OptionParser.new do |opts|
  opts.banner = "Usage: pub_perf [options]"

  opts.separator ""
  opts.separator "options:"

  opts.on("-n ITERATIONS", "iterations to send (default: #{$loop}}")     { |iter| $loop = iter.to_i }
  opts.on("-s SIZE", "Message size (default: #{$data.size})")            { |size| $data_size = size.to_i }
  opts.on("-S SUBJECT", "Send subject (default: (#{$sub})")              { |nsub| $sub = nsub }
  opts.on("-O Max outstanding", "Maximum number of outstanding bytes " +
          "(default: #{$max_outstanding/1024}k)")                        { |out|  $max_outstanding = out.to_i }
end

parser.parse(ARGV)
$to_drain = $loop - 1
$flow_trigger = $max_outstanding/4

trap("TERM") { exit! }
trap("INT")  { exit! }

NATS.on_error { |err| puts "Server Error: #{err}"; exit! }

NATS.start do

  $start = Time.now

  def done
    $to_drain-=1
    # Send last message and add closure which gets called when the server has processed
    # the message. This way we know all messages have been processed by the server.
    NATS.publish($sub, $data) {
      puts "\nTest completed : #{($loop/(Time.now-$start)).ceil} msgs/sec.\n"
      NATS.stop
    }
  end

  def drain
    sent_flow = false
    while ($to_drain > 0) do
      $to_drain-=1
      outstanding_bytes = NATS.client.get_outbound_data_size
      if (outstanding_bytes > $flow_trigger && !sent_flow)
        NATS.publish($sub, $data) { drain }
        sent_flow = true
      else
        NATS.publish($sub, $data)
      end
      printf('+') if $to_drain.modulo($hash) == 0
      break if outstanding_bytes > $max_outstanding
    end
    done if $to_drain == 0
  end

  if false
    EM.add_periodic_timer(0.1) do
      puts "Outstanding data size is #{NATS.client.get_outbound_data_size}"
    end
  end

  # kick things off..
  puts "Sending #{$loop} messages of size #{$data.size} bytes on [#{$sub}], outbound buffer maximum is #{$max_outstanding} bytes"
  drain

end
