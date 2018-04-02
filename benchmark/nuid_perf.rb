require 'securerandom'
require 'nats/nuid'
require 'benchmark/ips'

Benchmark.ips do |x|
  # x.report "NUID based inboxes with locked instance" do |t|
  #   t.times { "_INBOX.#{NATS::NUID.next}" }
  # end

  x.report "NUID based inboxes with owned instance" do |t|
    nuid = NATS::NUID.new
    t.times { "_INBOX.#{nuid.next}" }
  end

  x.report "SecureRandom based inboxes" do |t|
    t.times { "_INBOX.#{::SecureRandom.hex(11)}" }
  end
 
  x.compare!
end
