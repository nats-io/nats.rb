require 'nats/client'

EM.run do

  options = {
    :servers => [
      'nats://secret:deadbeef@127.0.0.1:4443',
      'nats://secret:deadbeef@127.0.0.1:4444'
    ],
    :max_reconnect_attempts => 10,
    :reconnect_time_wait => 2,
    :tls => {
      :private_key_file => './spec/configs/certs/key.pem',
      :cert_chain_file  => './spec/configs/certs/server.pem'
    }
  }

  NATS.connect(options) do |nc|
    puts "#{Time.now.to_f} - Connected to NATS at #{nc.connected_server}"

    nc.subscribe("hello") do |msg|
      puts "#{Time.now.to_f} - Received: #{msg}"
    end

    nc.flush do
      nc.publish("hello", "world")
    end

    EM.add_periodic_timer(0.1) do
      next unless nc.connected?
      nc.publish("hello", "hello")
    end

    # Set default callbacks
    nc.on_error do |e|
      puts "#{Time.now.to_f } - Error: #{e}"
    end

    nc.on_disconnect do |reason|
      puts "#{Time.now.to_f} - Disconnected: #{reason}"
    end

    nc.on_reconnect do |nc|
      puts "#{Time.now.to_f} - Reconnected to NATS server at #{nc.connected_server}"
    end

    nc.on_close do
      puts "#{Time.now.to_f} - Connection to NATS closed"
      EM.stop
    end
  end
end
