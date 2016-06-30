require 'nats/client'

options = {
  :servers => [
   'nats://secret:deadbeef@127.0.0.1:4443',
   'nats://secret:deadbeef@127.0.0.1:4444'
  ],
  :max_reconnect_attempts => 10,
  :reconnect_time_wait => 2,
  :tls => {
    :ssl_version => :TLSv1_2,
    :protocols => [:tlsv1_2],
    :private_key_file => './spec/configs/certs/key.pem',
    :cert_chain_file  => './spec/configs/certs/server.pem'
  }
}

# Set default callbacks
NATS.on_error do |e|
  puts "#{Time.now.to_f } - Error: #{e}"
end

NATS.on_disconnect do |reason|
  puts "#{Time.now.to_f} - Disconnected: #{reason}"
end

NATS.on_reconnect do |next_server_uri|
  puts "#{Time.now.to_f} - Trying to reconnect to NATS server at #{next_server_uri}"
end

NATS.on_close do
  puts "#{Time.now.to_f} - Connection to NATS closed"
  EM.stop
end

NATS.start(options) do |nats|
  puts "#{Time.now.to_f} - Connected to NATS at #{nats.connected_server}"

  nats.subscribe("hello") do |msg|
    puts "#{Time.now.to_f} - Received: #{msg}"
  end

  nats.flush do
    nats.publish("hello", "world")
  end

  EM.add_periodic_timer(0.1) do
    next unless nats.connected?
    nats.publish("hello", "hello")
  end
end
