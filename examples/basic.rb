require 'nats/io/client'

nats = NATS::IO::Client.new

nats.connect(:servers => ["nats://127.0.0.1:4222"])
puts "Connected to #{nats.connected_server}"

nats.subscribe(">") do |msg, reply, subject|
  puts "Received on '#{subject} #{reply}': #{msg}"
  nats.publish(reply, "A" * 100) if reply
end

total = 0
payload = "b"
loop do
  nats.publish("hello.#{total}", payload)

  begin
    nats.flush(1)

    # Request which waits until given a response or a timeout
    msg = nats.request("hello", "world")
    puts "Received on '#{msg[:subject]} #{msg[:reply]}': #{msg[:data]}"

    total += 1
    sleep 0.0001 if total % 1000 == 0
  rescue NATS::IO::Timeout
    puts "ERROR: flush timeout"
  end
end
