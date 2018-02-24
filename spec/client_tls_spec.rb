require 'spec_helper'
require 'openssl'
require 'erb'

describe 'Client - TLS spec' do

  context 'when server requires TLS and no auth needed' do
    before(:each) do
      opts = {
        'pid_file' => '/tmp/test-nats-4444.pid',
        'host' => '127.0.0.1',
        'port' => 4444
      }
      config = ERB.new(%Q(
        net:  "<%= opts['host'] %>"
        port: <%= opts['port'] %>

        tls {
          cert_file:  "./spec/configs/certs/server.pem"
          key_file:   "./spec/configs/certs/key.pem"
          timeout:    10

          <% if RUBY_PLATFORM == "java" %>
          # JRuby is sensible to the ciphers being used
          # so we specify the ones that are available on it here.
          # See: https://github.com/jruby/jruby/issues/1738
          cipher_suites: [
            "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
            "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA",
            "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA",
            "TLS_RSA_WITH_AES_128_CBC_SHA",
            "TLS_RSA_WITH_AES_256_CBC_SHA",
            "TLS_RSA_WITH_3DES_EDE_CBC_SHA"
          ]
          <% end %>
      }))
      @tls_no_auth = NatsServerControl.init_with_config_from_string(config.result(binding), opts)
      @tls_no_auth.start_server
    end

    after(:each) do
      @tls_no_auth.kill_server
    end

    it 'should error if client does not set secure connection and server requires it' do
      errors = []
      closes = 0
      reconnects = 0
      disconnects = 0
      reconnects = 0

      nats = NATS::IO::Client.new
      nats.on_close      { closes += 1 }
      nats.on_reconnect  { reconnects += 1 }
      nats.on_disconnect { disconnects += 1 }
      nats.on_error do |e|
        errors << e
      end

      expect do
        nats.connect(:servers => ['nats://127.0.0.1:4444'], :reconnect => false)
      end.to raise_error(NATS::IO::ConnectError)

      # Async handler also gets triggered since defined
      expect(errors.count).to eql(1)
      expect(errors.first).to be_a(NATS::IO::ConnectError)

      # No close since we were not even connected
      expect(closes).to eql(0)
      expect(reconnects).to eql(0)
      expect(disconnects).to eql(1)
    end

    it 'should allow to connect client with secure connection if server requires it' do
      errors = []
      closes = 0
      reconnects = 0
      disconnects = 0
      reconnects = 0

      nats = NATS::IO::Client.new
      nats.on_close      { closes += 1 }
      nats.on_reconnect  { reconnects += 1 }
      nats.on_disconnect { disconnects += 1 }
      nats.on_error do |e|
        errors << e
      end

      expect do
        nats.connect(:servers => ['tls://127.0.0.1:4444'], :reconnect => false)
      end.to_not raise_error

      # Confirm basic secure publishing works
      msgs = []
      nats.subscribe("hello.*") do |msg|
        msgs << msg
      end
      nats.flush

      # Send some messages...
      100.times {|n| nats.publish("hello.#{n}", "world") }
      nats.flush
      sleep 0.5

      # Gracefully disconnect
      nats.close

      # Should have published 100 messages without errors
      expect(msgs.count).to eql(100)
      expect(errors.count).to eql(0)
      expect(closes).to eql(1)
      expect(reconnects).to eql(0)
      expect(disconnects).to eql(1)
    end

    it 'should allow custom secure connection contexts' do
      errors = []
      closes = 0
      reconnects = 0
      disconnects = 0
      reconnects = 0

      nats = NATS::IO::Client.new
      nats.on_close      { closes += 1 }
      nats.on_reconnect  { reconnects += 1 }
      nats.on_disconnect { disconnects += 1 }
      nats.on_error do |e|
        errors << e
      end

      expect do
        tls_context = OpenSSL::SSL::SSLContext.new
        tls_context.ssl_version = :TLSv1

        nats.connect({
                       servers: ['tls://127.0.0.1:4444'],
                       reconnect: false,
                       tls: {
                         context: tls_context
                       }
                     })
      end.to raise_error(OpenSSL::SSL::SSLError)
    end
  end

  context 'when server requires TLS and certificates' do
    before(:each) do
      opts = {
        'pid_file' => '/tmp/test-nats-4555.pid',
        'host' => '127.0.0.1',
        'port' => 4555
      }
      config = ERB.new(%Q(
        net:  "<%= opts['host'] %>"
        port: <%= opts['port'] %>

        tls {
          cert_file:  "./spec/configs/certs/server.pem"
          key_file:   "./spec/configs/certs/key.pem"
          timeout:    10

          # Optional certificate authority for clients
          ca_file:   "./spec/configs/certs/ca.pem"

          # Require a client certificate
          verify:    true


          <% if RUBY_PLATFORM == "java" %>
          # JRuby is sensible to the ciphers being used
          # so we specify the ones that are available on it here.
          # See: https://github.com/jruby/jruby/issues/1738
          cipher_suites: [
            "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
            "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA",
            "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA",
            "TLS_RSA_WITH_AES_128_CBC_SHA",
            "TLS_RSA_WITH_AES_256_CBC_SHA",
            "TLS_RSA_WITH_3DES_EDE_CBC_SHA"
          ]
          <% end %>
      }))
      @tlsverify = NatsServerControl.init_with_config_from_string(config.result(binding), opts)
      @tlsverify.start_server
    end
    after(:each) do
      @tlsverify.kill_server
    end

    it 'should allow custom secure connection contexts' do
      errors = []
      closes = 0
      reconnects = 0
      disconnects = 0
      reconnects = 0

      nats = NATS::IO::Client.new
      nats.on_close      { closes += 1 }
      nats.on_reconnect  { reconnects += 1 }
      nats.on_disconnect { disconnects += 1 }
      nats.on_error do |e|
        errors << e
      end

      expect do
        tls_context = OpenSSL::SSL::SSLContext.new
        tls_context.cert = OpenSSL::X509::Certificate.new File.open("./spec/configs/certs/client-cert.pem")
        tls_context.key = OpenSSL::PKey::RSA.new File.open("./spec/configs/certs/client-key.pem")
        tls_context.ca_file = "./spec/configs/certs/ca.pem"
        tls_context.verify_mode = OpenSSL::SSL::VERIFY_PEER

        nats.connect({
          servers: ['tls://127.0.0.1:4555'],
          reconnect: false,
          tls: {
            context: tls_context
          }
        })

        nats.subscribe("hello") do |msg, reply|
          nats.publish(reply, 'ok')
        end

        response = nats.request("hello", "world")
        expect(response.data).to eql("ok")
      end.to_not raise_error
    end
  end

  context 'when server requires TLS and client enables host verification', :tls_verify_hostname do
    let(:tls_context) {
      ctx = OpenSSL::SSL::SSLContext.new
      ctx.ca_file = "./spec/configs/certs/nats-service.localhost/ca.pem"
      ctx.verify_mode = OpenSSL::SSL::VERIFY_PEER
      ctx.verify_hostname = true

      ctx
    }
    before(:each) do
      @tls_verify_host_server_uri = URI.parse("nats://127.0.0.1:4556")
      opts = {
        'pid_file' => "/tmp/test-nats-#{@tls_verify_host_server_uri.port}.pid",
        'host' => '127.0.0.1',
        'port' => @tls_verify_host_server_uri.port
      }
      config = ERB.new(%Q(
        net:  "<%= opts['host'] %>"
        port: <%= opts['port'] %>

        tls: {
          cert_file:  "./spec/configs/certs/nats-service.localhost/server.pem"
          key_file:   "./spec/configs/certs/nats-service.localhost/server-key.pem"
          timeout:    10

          # Optional certificate authority for clients
          ca_file:   "./spec/configs/certs/nats-service.localhost/ca.pem"

          # Require a client certificate
          # verify:    true

          <% if RUBY_PLATFORM == "java" %>
          <%= DEFAULT_JRUBY_CIPHER_SUITE %>
          <% end %>
      }))
      @tls_verify_host = NatsServerControl.init_with_config_from_string(config.result(binding), opts)
      @tls_verify_host.start_server
    end

    after(:each) do
      @tls_verify_host.kill_server
    end

    it 'should be able to connect and verify server hostname' do
      expect do
        nats = NATS::IO::Client.new

        nats.connect({
          servers: ["tls://server-A.clients.nats-service.localhost:#{@tls_verify_host_server_uri.port}"],
          reconnect: false,
          tls: {
            context: tls_context
          }
        })

        nats.subscribe("hello") do |msg, reply|
          nats.publish(reply, 'ok')
        end

        response = nats.request("hello", "world")
        expect(response.data).to eql("ok")
      end.to_not raise_error
    end

    it 'should not be able to connect if using wrong server hostname' do
      expect do
        nats = NATS::IO::Client.new

        nats.connect({
          servers: ["tls://server-A.clients.fake-nats-service.localhost:#{@tls_verify_host_server_uri.port}"],
          reconnect: false,
          tls: {
            context: tls_context
          }
        })

        nats.subscribe("hello") do |msg, reply|
          nats.publish(reply, 'ok')
        end

        response = nats.request("hello", "world")
        expect(response.data).to eql("ok")
      end.to raise_error(OpenSSL::SSL::SSLError)
    end
  end

  context 'when bad server requires TLS', :tls_verify_hostname do
    before(:each) do
      @tls_verify_host_bad_server_uri = URI.parse("nats://127.0.0.1:4557")
      opts = {
        'pid_file' => "/tmp/test-nats-#{@tls_verify_host_bad_server_uri.port}.pid",
        'host' => '127.0.0.1',
        'port' => @tls_verify_host_bad_server_uri.port
      }
      config = ERB.new(%Q(
        net:  "<%= opts['host'] %>"
        port: <%= opts['port'] %>

        tls: {
          cert_file:  "./spec/configs/certs/nats-service.localhost/fakeserver.pem"
          key_file:   "./spec/configs/certs/nats-service.localhost/fakeserver-key.pem"
          timeout:    10

          # Optional certificate authority for clients
          ca_file:   "./spec/configs/certs/nats-service.localhost/ca.pem"

          # Require a client certificate
          # verify:    true

          <% if RUBY_PLATFORM == "java" %>
          <%= DEFAULT_JRUBY_CIPHER_SUITE %>
          <% end %>
      }))
      @tls_verify_bad_host = NatsServerControl.init_with_config_from_string(config.result(binding), opts)
      @tls_verify_bad_host.start_server
    end

    after(:each) do
      @tls_verify_bad_host.kill_server
    end

    it 'should be able to connect if client does not verify server hostname' do
      ctx = OpenSSL::SSL::SSLContext.new
      ctx.ca_file = "./spec/configs/certs/nats-service.localhost/ca.pem"
      ctx.verify_mode = OpenSSL::SSL::VERIFY_PEER
      ctx.verify_hostname = false

      expect do
        nats = NATS::IO::Client.new

        nats.connect({
          servers: ["tls://server-A.clients.nats-service.localhost:#{@tls_verify_host_bad_server_uri.port}"],
          reconnect: false,
          tls: {
            context: ctx
          }
        })

        nats.subscribe("hello") do |msg, reply|
          nats.publish(reply, 'ok')
        end

        response = nats.request("hello", "world")
        expect(response.data).to eql("ok")
      end.to_not raise_error
    end

    it 'should not be able to connect if client enableds hostname verification' do
      ctx = OpenSSL::SSL::SSLContext.new
      ctx.ca_file = "./spec/configs/certs/nats-service.localhost/ca.pem"
      ctx.verify_mode = OpenSSL::SSL::VERIFY_PEER
      ctx.verify_hostname = true

      expect do
        nats = NATS::IO::Client.new

        nats.connect({
          servers: ["tls://server-A.clients.nats-service.localhost:#{@tls_verify_host_bad_server_uri.port}"],
          reconnect: false,
          tls: {
            context: ctx
          }
        })

        nats.subscribe("hello") do |msg, reply|
          nats.publish(reply, 'ok')
        end

        response = nats.request("hello", "world")
        expect(response.data).to eql("ok")
      end.to raise_error(OpenSSL::SSL::SSLError)
    end
  end
end
