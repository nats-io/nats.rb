
require 'optparse'
require File.dirname(__FILE__) + '/../lib/nats/client'

$loop = 10000
$hash = 1000
$sub  = 'test'

STDOUT.sync = true

parser = OptionParser.new do |opts|
  opts.banner = "Usage: latency_perf [options]"

  opts.separator ""
  opts.separator "options:"

  opts.on("-n ITERATIONS", "iterations to expect (default: #{$expected}")    { |iter| $loop = iter.to_i }
end

parser.parse(ARGV)
$drain = $loop-1

trap("TERM") { exit! }
trap("INT")  { exit! }

NATS.on_error { |err| puts "Server Error: #{err}"; exit! }

NATS.start do

  s_conn = NATS.connect

  s_conn.subscribe('test') do |sub, msg, reply|
    s_conn.publish(reply)
  end

  def done
    ms = "%.2f" % (((Time.now-$start)/$loop)*1000.0)
    puts "\nTest completed : #{ms} ms avg request/response latency\n"
    NATS.stop
  end

  def send_request
    NATS.request('test') {
      $drain-=1
      if $drain == 0
        done
      else
        send_request
        printf('+') if $drain.modulo($hash) == 0
      end
    }
  end

  $start = Time.now
  puts "Sending #{$loop} request/responses"
  # Send first request
  send_request
end
