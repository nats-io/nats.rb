require 'nats/io/client'

nats = NATS::IO::Client.new

nats.on_reconnect do
  puts "Reconnected to server at #{nats.connected_server}"
end

nats.on_disconnect do
  puts "Disconnected!"
end

nats.on_close do
  puts "Connection to NATS closed"
end

cluster_opts = {
  servers: ["nats://127.0.0.1:4222", "nats://127.0.0.1:4223"],
  dont_randomize_servers: true,
  reconnect_time_wait: 0.5,
  max_reconnect_attempts: 2
}

nats.connect(cluster_opts)
puts "Connected to #{nats.connected_server}"

nats.subscribe("hello") do |data|
  puts "#{Time.now} - Received: #{data}"
end

n = 0
loop do
  n += 1
  nats.publish("hello", "world.#{n}")
  sleep 0.1
end
