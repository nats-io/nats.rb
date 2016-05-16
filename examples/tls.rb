require 'nats/client'

EM.run do
  # Set default callbacks
  NATS.on_error do |e|
    # next if e.kind_of?(NATS::ConnectError)
    puts "#{Time.now.to_f } - Error: #{e}"
  end

  NATS.on_disconnect do
    puts "#{Time.now.to_f} - Disconnected from NATS"
  end

  NATS.on_reconnect do
    puts "#{Time.now.to_f} - Reconnected to NATS"
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
    :max_reconnect_attempts => 3,
    :reconnect_time_wait => 2,
    :tls => {
      :ssl_version => :TLSv1_2,
      :protocols => [:tlsv1_2],
      :private_key_file => './spec/configs/certs/key.pem',
      :cert_chain_file  => './spec/configs/certs/server.pem',
      :verify_peer      => false
    }
  }

  nc = NATS.connect(options) do
    puts "#{Time.now.to_f} - Connected to NATS"
  end
  messages = []
  sid = nc.subscribe("hello") do |msg|
    puts "Received: #{msg}"
  end
  nc.flush do
    nc.publish("hello", "world") do
      nc.unsubscribe(sid)
      nc.flush do
        nc.close
      end
    end
  end
end
