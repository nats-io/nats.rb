
require File.dirname(__FILE__) + '/../lib/nats/client'

data = '-ok-'.freeze

NATS.on_error { |err| puts "Server Error: #{err}"; exit }

NATS.start do |c|
  
  r, loop = 0, 250000
  start = Time.now 
  
  do_subs = true if ARGV.shift =~ /-listen/i
  
  c.subscribe('test') { |sub, msg| r += 1 } if do_subs
  
  c.subscribe(:done) do |sub, msg|
    stop = Time.now    
    puts "Stop msg received on [#{sub}] : '#{msg}'"
    puts "Test completed."
    puts "Received #{r} of #{loop} expected msgs" if do_subs
    puts "Nats processed #{(loop/(stop-start)).ceil} msgs/sec.\n"
    NATS.stop
  end

  # Send data loop times
  loop.times { c.publish(:test, data) }
  
  # end condition trigger
  c.publish(:done, 'ok')

  puts "All #{loop} messages sent from client at a perceived rate of #{(loop/(Time.now-start)).ceil} msgs/sec.\n" 

end
