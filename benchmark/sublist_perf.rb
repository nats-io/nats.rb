
$:.unshift File.expand_path('../../lib', __FILE__)
require 'nats/server/sublist'

class PerfSublist
  @@levels  = 5
  @@targets = ['derek', 'ruth', 'sam', 'meg', 'brett', 'ben', 'miles', 'bella', 'rex', 'diamond']
  @@sublist = Sublist.new()
  @@subs    = []

  def PerfSublist.subsInit(pre=nil)
    @@targets.each {|t|
        sub = pre ? (pre + "." + t) : t
        @@sublist.insert(sub, sub)
        @@subs << sub
        subsInit(sub) if sub.split(".").size < @@levels
      }
  end

  def PerfSublist.disableSublistCache
    @@sublist.disable_cache
  end

  def PerfSublist.addWildcards
    @@sublist.insert("ruth.>", "honey")
    @@sublist.insert("ruth.sam.meg.>", "honey")
    @@sublist.insert("ruth.*.meg.*", "honey")
  end

  def PerfSublist.matchTest(subject, loop)
    start = Time.now
    loop.times {@@sublist.match(subject)}
    stop = Time.now
    puts "Matched #{subject} #{loop} times in #{ms_time(stop-start)} ms @ #{(loop/(stop-start)).to_i}/sec"
  end

  def PerfSublist.reset
    @@sublist = Sublist.new()
  end

  def PerfSublist.removeAll
    @@subs.each do |sub|
      @@sublist.remove(sub, sub)
    end
  end

  def PerfSublist.subscriptionCount
    @@sublist.count
  end

  def PerfSublist.totalCount
    @@subs.count
  end

end

def ms_time(t)
  "%0.2f" % (t*1000)
end

# setup the subscription list.
start = Time.now
PerfSublist.subsInit
PerfSublist.addWildcards
stop = Time.now
puts
puts "Sublist holding #{PerfSublist.subscriptionCount} subscriptions"
puts "Insert rate of #{(PerfSublist.subscriptionCount/(stop-start)).to_i}/sec"

#require 'profiler'

puts
puts "cache test"

#Profiler__::start_profile
PerfSublist.matchTest("derek.sam.meg.ruth", 100000)
PerfSublist.matchTest("ruth.sam.meg.derek", 100000) # multiple returns w/ wc
PerfSublist.matchTest("derek.sam.meg.billybob", 100000) # worst case miss
#Profiler__::stop_profile
#Profiler__::print_profile($stdout)

puts
puts "Hit any key to continue w/ cache disabled"
STDIN.getc

#Profiler__::start_profile
PerfSublist.disableSublistCache
PerfSublist.matchTest("derek.sam.meg.ruth", 50000)
PerfSublist.matchTest("ruth.sam.meg.derek", 50000) # multiple returns w/ wc
PerfSublist.matchTest("derek.sam.meg.billybob", 50000) # worst case miss
#Profiler__::stop_profile
#Profiler__::print_profile($stdout)

#Run multiple times to see Jruby speedup
0.times do
  puts "\n\n"
  #PerfSublist.reset
  #PerfSublist.subsInit
  #PerfSublist.disableSublistCache
  PerfSublist.matchTest("derek.sam.meg.ruth", 50000)
  PerfSublist.matchTest("ruth.sam.meg.derek", 50000) # multiple returns w/ wc
  PerfSublist.matchTest("derek.sam.meg.billybob", 50000) # worst case miss
end

start = Time.now
PerfSublist.removeAll
stop = Time.now
puts
puts "Sublist now holding #{PerfSublist.subscriptionCount} subscriptions"
puts "Removal rate of #{(PerfSublist.totalCount/(stop-start)).to_i}/sec"

# Allows you to see memory usage, etc
puts
puts "Hit any key to quit"
STDIN.getc
