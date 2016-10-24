require 'nats/io/client'

$stdout.sync = true
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
  servers: ["nats://127.0.0.1:4222", "nats://127.0.0.1:4223","nats://127.0.0.1:4224"],
  dont_randomize_servers: true,
  reconnect_time_wait: 1,
  max_reconnect_attempts: 5
}

nats.connect(cluster_opts)
puts "Connected to #{nats.connected_server}"

msgs_sent = 0
msgs_received = 0
bytes_sent = 0
bytes_received = 0

nats.subscribe("hello") {|data| msgs_received += 1; bytes_received += data.size }

Thread.new do
  loop do
    puts "#{Time.now} #{Thread.list.count} - [Sent/Received] #{msgs_sent}/#{msgs_received} msgs (#{msgs_sent - msgs_received}) | [Received] #{bytes_sent}/#{bytes_received} B (#{bytes_sent - bytes_received})"
    p Thread.list
    sleep 1
  end
end

loop do
  payload = "world.#{msgs_sent}"
  nats.publish("hello", payload)
  msgs_sent += 1
  bytes_sent += payload.size
  sleep 0.00001
end
