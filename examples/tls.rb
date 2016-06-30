require 'nats/client'

EM.run do

  # Set default callbacks
  NATS.on_error do |e|
    puts "#{Time.now.to_f } - Error: #{e}"
  end

  NATS.on_disconnect do
    puts "#{Time.now.to_f} - Disconnected from NATS"
  end

  NATS.on_reconnect do |next_server_uri|
    puts "#{Time.now.to_f} - Trying reconnect to NATS at #{next_server_uri}"
  end

  NATS.on_close do
    puts "#{Time.now.to_f} - Connection to NATS closed"
    EM.stop
  end

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

  nc = NATS.connect(options) do |nc|
    puts "#{Time.now.to_f} - Connected to NATS at #{nc.connected_server}"

    nc.subscribe("hello") do |msg|
      puts "#{Time.now.to_f} - Received: #{msg}"
    end

    nc.flush do
      nc.publish("hello", "world")
    end
  end

  EM.add_periodic_timer(0.1) do
    next unless nc or nc.connected?
    nc.publish("hello", "hello")
  end
end
