require 'nats/io/client'
require 'openssl'

nats = NATS::IO::Client.new

nats.on_error do |e|
  puts "Error: #{e}"
  puts e.backtrace
end

tls_context = OpenSSL::SSL::SSLContext.new
tls_context.ssl_version = :TLSv1_2

# Deactivating this makes it possible to connect to server
# skipping hostname validation...
tls_context.verify_mode = OpenSSL::SSL::VERIFY_PEER
tls_context.verify_hostname = true

# Set the RootCAs
tls_context.cert_store = OpenSSL::X509::Store.new
ca_file = File.read("./spec/configs/certs/nats-service.localhost/ca.pem")
tls_context.cert_store.add_cert(OpenSSL::X509::Certificate.new ca_file)

# The server is setup to use a wildcard certificate for:
# 
# *.clients.nats-service.localhost
#
# so given the options above, having a wrong domain would
# make the client connection fail with an error
# 
# Error: SSL_connect returned=1 errno=0 state=error: certificate verify failed (error number 1)
# 
server = "nats://server-A.clients.nats-service.localhost:4222"

nats.connect(servers: [server], tls: { context: tls_context })
puts "Connected to #{nats.connected_server}"

nats.subscribe(">") do |msg, reply, subject|
  puts "Received on '#{subject} #{reply}': #{msg}"
  nats.publish(reply, "A" * 100) if reply
end

total = 0
payload = "c"
loop do
  nats.publish("hello.#{total}", payload)

  begin
    nats.flush(1)

    # Request which waits until given a response or a timeout
    msg = nats.request("hello", "world")
    puts "Received on '#{msg.subject} #{msg.reply}': #{msg.data}"

    total += 1
    sleep 1
  rescue NATS::IO::Timeout
    puts "ERROR: flush timeout"
  end
end
