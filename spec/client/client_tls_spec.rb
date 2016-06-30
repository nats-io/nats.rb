require 'spec_helper'

describe 'Client - TLS spec' do

  context 'when server does not support TLS' do

    before(:each) do
      @non_tls_server = NatsServerControl.new("nats://127.0.0.1:4222")
      @non_tls_server.start_server
    end

    after(:each) do
      @non_tls_server.kill_server
    end

    it 'should error if client requires TLS' do
      errors = []
      closes = 0
      reconnects = 0
      disconnects = 0

      options = {
        :uri => 'nats://127.0.0.1:4222',
        :reconnect => false,
        :tls => {
          :ssl_version => :TLSv1_2,
          :protocols => [:tlsv1_2],
          :private_key_file => './spec/configs/certs/key.pem',
          :cert_chain_file  => './spec/configs/certs/server.pem',
          :verify_peer      => false
        }
      }

      with_em_timeout(5) do |future|
        nc = nil
        NATS.on_error      {|e| errors << e }
        NATS.on_close      { closes += 1 }
        NATS.on_reconnect  { reconnects += 1 }
        NATS.on_disconnect { disconnects += 1 }

        nc = NATS.connect(options)
      end

      expect(errors.count).to eql(2)
      expect(errors.first).to be_a(NATS::ClientError)
      expect(errors.first.to_s).to eql("TLS/SSL not supported by server")
      expect(errors.last).to be_a(NATS::ConnectError)
      expect(closes).to eql(1)
      expect(reconnects).to eql(0)

      # Technically we were never connected to the NATS service
      # in that server so we don't call disconnect right now.
      expect(disconnects).to eql(0)
    end
  end

  context 'when server requires TLS and no auth needed' do

    before(:each) do
      @tls_no_auth = NatsServerControl.new("nats://127.0.0.1:4444", '/tmp/test-nats-4444.pid', "-c ./spec/configs/tls-no-auth.conf")
      @tls_no_auth.start_server
    end

    after(:each) do
      @tls_no_auth.kill_server
    end

    it 'should error if client does not set secure connection and dispatch callbacks' do
      errors = []
      closes = 0
      reconnects = 0
      disconnects = 0
      reconnects = 0

      with_em_timeout(3) do |future|
        nc = nil

        NATS.on_close      { closes += 1 }
        NATS.on_reconnect  { reconnects += 1 }
        NATS.on_disconnect { disconnects += 1 }

        NATS.on_error do |e|
          errors << e
        end
        nc = NATS.connect(:uri => 'nats://127.0.0.1:4444', :reconnect => false)
      end

      expect(errors.count > 0).to eq(true)
      expect(errors.first).to be_a(NATS::ClientError)
      expect(closes).to eql(1)
      expect(reconnects).to eql(0)
      expect(disconnects).to eql(1)
    end
  end

  context 'when server requires TLS and authentication' do

    before(:each) do
      @tls_auth = NatsServerControl.new("nats://127.0.0.1:4443", '/tmp/test-nats-4443.pid', "-c ./spec/configs/tls.conf")
      @tls_auth.start_server
    end

    after(:each) do
      @tls_auth.kill_server
    end

    it 'should error if client does not set secure connection and dispatch callbacks' do
      errors = []
      with_em_timeout(3) do |future|
        nc = nil
        NATS.on_error do|e|
          errors << e
        end
        nc = NATS.connect(:uri => 'nats://127.0.0.1:4443', :reconnect => false)
      end

      # Client disconnected from server
      expect(errors.count > 0).to eq(true)
      expect(errors.first).to be_a(NATS::ClientError)
    end

    it 'should error if client does not set secure connection and stop trying to reconnect eventually' do
      errors = []
      reconnects = 0
      disconnects = 0
      closes = 0

      with_em_timeout(10) do |future|
        nc = nil

        NATS.on_error do |e|
          errors << e
        end

        NATS.on_disconnect do
          disconnects += 1
        end

        NATS.on_reconnect do |conn|
          expect(conn.connected_server).to eql(URI.parse('nats://127.0.0.1:4443'))
          reconnects += 1
        end

        NATS.on_close do
          # NOTE: We cannot close again here in tests since
          # we would be getting double fiber called errors.
          # future.resume(nc) if not nc.closing?
          closes += 1
          future.resume
        end

        nc = NATS.connect({
          :servers => ['nats://127.0.0.1:4443'],
          :max_reconnect_attempts => 2,
          :reconnect_time_wait => 1
        })
      end

      # FIXME: It will be trying to reconnect for a number of times
      # and some of the erros that will be getting could be errors
      # such as Unknown Protocol due to parser failing with secure conn.
      expect(reconnects).to eql(3)
      expect(disconnects).to eql(4)
      expect(closes).to eql(1)
      expect(errors.count > 0).to eq(true)
      expect(errors.count < 10).to eq(true)

      # Client disconnected from server
      expect(errors.first).to be_a(NATS::ClientError)
      expect(errors.last).to be_a(NATS::ConnectError)
    end

    it 'should reject secure connection when using deprecated versions' do
      errors = []
      connects = 0
      reconnects = 0
      disconnects = 0
      closes = 0

      with_em_timeout(10) do |future|
        nc = nil

        NATS.on_error do |e|
          errors << e
        end

        NATS.on_disconnect do
          disconnects += 1
        end

        NATS.on_reconnect do
          reconnects += 1
        end

        NATS.on_close do
          closes += 1
        end

        nc = NATS.connect({
          :servers => ['nats://secret:deadbeef@127.0.0.1:4443'],
          :tls => {
            :ssl_version => :sslv2
          }}) do
          connects += 1
        end
        nc.subscribe("hello")
        nc.flush do
          nc.close
        end
      end
      expect(errors.count).to eql(1)
      expect(errors.first).to be_a(NATS::ConnectError)
      expect(connects).to eql(0)
      expect(closes).to eql(1)
      expect(disconnects).to eql(0)
      expect(reconnects).to eql(0)
    end

    it 'should connect securely to server and authorize with default TLS and protocols options' do
      errors      = []
      messages    = []
      connects    = 0
      disconnects = 0
      reconnects  = 0
      closes      = 0

      with_em_timeout(10) do
        NATS.on_error do |e|
          errors << e
        end

        NATS.on_disconnect do
          disconnects += 1
        end

        NATS.on_reconnect do
          reconnects += 1
        end

        NATS.on_close do
          closes += 1
        end

        # Empty dict also enables TLS handling with defaults.
        options = {
          :servers => ['nats://secret:deadbeef@127.0.0.1:4443'],
          :max_reconnect_attempts => 1,
          :dont_randomize_servers => true,
          :tls => { }
        }

        nc = NATS.connect(options) do |nc2|
          expect(nc2.connected_server).to eql(URI.parse('nats://secret:deadbeef@127.0.0.1:4443'))
          connects += 1
        end

        nc.subscribe("hello") do |msg|
          messages << msg
        end
        nc.flush do
          nc.publish("hello", "world") do
            nc.unsubscribe("hello")
            nc.close
            expect(messages.count).to eql(1)
          end
        end
      end
      expect(errors.count).to eql(0)

      # We are calling close so should not be calling
      # the disconnect callback here.
      expect(disconnects).to eql(0)
      expect(connects).to eql(1)
      expect(closes).to eql(1)
      expect(reconnects).to eql(0)
    end

    it 'should connect securely to server and authorize with defaults only via setting ssl enabled option' do
      errors      = []
      messages    = []
      connects    = 0
      disconnects = 0
      reconnects  = 0
      closes      = 0

      with_em_timeout(10) do
        NATS.on_error do |e|
          errors << e
        end

        NATS.on_disconnect do
          disconnects += 1
        end

        NATS.on_reconnect do
          reconnects += 1
        end

        NATS.on_close do
          closes += 1
        end

        options = {
          :servers => ['nats://secret:deadbeef@127.0.0.1:4443'],
          :max_reconnect_attempts => 1,
          :dont_randomize_servers => true,
          :ssl => true
        }

        nc = NATS.connect(options) do |conn|
          expect(conn.connected_server).to eql(URI.parse('nats://secret:deadbeef@127.0.0.1:4443'))
          connects += 1
        end

        nc.subscribe("hello") do |msg|
          messages << msg
        end
        nc.flush do
          nc.publish("hello", "world") do
            nc.unsubscribe("hello")
            nc.close
          end
        end
      end
      expect(messages.count).to eql(1)
      expect(errors.count).to eql(0)

      # We are calling close so should not be calling
      # the disconnect callback here.
      expect(disconnects).to eql(0)
      expect(closes).to eql(1)
      expect(reconnects).to eql(0)
    end

    it 'should connect securely with default TLS and protocols options' do
      errors      = []
      messages    = []
      connects    = 0
      reconnects  = 0
      disconnects = 0
      closes      = 0

      with_em_timeout(10) do |future|
        NATS.on_error do |e|
          errors << e
        end

        NATS.on_disconnect do |e|
          disconnects += 1
        end

        NATS.on_close do
          closes += 1
        end

        NATS.on_reconnect do
          reconnects += 1
        end

        options = {
          :servers => ['nats://secret:deadbeef@127.0.0.1:4443'],
          :max_reconnect_attempts => 1,
          :dont_randomize_servers => true,
          :tls => {
            # :ssl_version => :TLSv1_2,
            # :protocols => [:tlsv1_2],
            # :private_key_file => './spec/configs/certs/key.pem',
            # :cert_chain_file  => './spec/configs/certs/server.pem',
            # :verify_peer => true
          }
        }

        nc = NATS.connect(options) do |conn|
          expect(conn.connected_server).to eql(URI.parse('nats://secret:deadbeef@127.0.0.1:4443'))
          connects += 1
        end

        nc.subscribe("hello") do |msg|
          messages << msg
        end
        nc.flush do
          nc.publish("hello", "world") do
            nc.unsubscribe("hello")
            nc.close
          end
        end
      end
      expect(errors.count).to eql(0)
      expect(messages.count).to eql(1)
      expect(reconnects).to eql(0)
      expect(closes).to eql(1)
      expect(disconnects).to eql(0)
    end
  end

  context 'when server requires TLS, certificates and authentication' do

    before(:each) do
      @tls_verify_auth = NatsServerControl.new("nats://127.0.0.1:4445", '/tmp/test-nats-4445.pid', "-c ./spec/configs/tlsverify.conf")
      @tls_verify_auth.start_server
    end

    after(:each) do
      @tls_verify_auth.kill_server
    end

    it 'should error if client does not set secure connection and dispatch callbacks' do
      errors = []
      with_em_timeout(10) do |future|
        nc = nil
        NATS.on_error do |e|
          errors << e
        end
        nc = NATS.connect(:uri => 'nats://127.0.0.1:4445', :reconnect => false)
      end
      expect(errors.count >= 2).to eql(true)

      # Client disconnected from server
      expect(errors.first).to be_a(NATS::ClientError)
      expect(errors.last).to  be_a(NATS::ConnectError)
    end

    it 'should error if client does not set secure connection and stop trying to reconnect eventually' do
      errors = []
      reconnects = 0
      disconnects = 0
      closes = 0

      with_em_timeout(10) do |future|
        nc = nil

        NATS.on_error do |e|
          errors << e
        end

        NATS.on_disconnect do
          disconnects += 1
        end

        NATS.on_reconnect do
          reconnects += 1
        end

        NATS.on_close do
          # NOTE: We cannot close again here in tests since
          # we would be getting double fiber called errors.
          # future.resume(nc) if not nc.closing?
          closes += 1
        end

        nc = NATS.connect(:uri => 'nats://127.0.0.1:4445', :reconnect_time_wait => 1, :max_reconnect_attempts => 2)
      end
      expect(errors.count > 2).to eql(true)
      expect(errors.count < 10).to eql(true)
      expect(disconnects).to eql(4)
      expect(reconnects).to eql(3)
      expect(closes).to eql(1)

      # Client disconnected from server
      expect(errors.first).to be_a(NATS::ClientError)
      expect(errors.last).to  be_a(NATS::ConnectError)
    end

    it 'should reject secure connection if no certificate is provided' do
      errors      = []
      connects    = 0
      reconnects  = 0
      disconnects = 0
      closes      = 0

      with_em_timeout(5) do |future|
        nc = nil

        NATS.on_error do |e|
          errors << e
        end

        NATS.on_disconnect do |e|
          disconnects += 1
        end

        NATS.on_reconnect do
          reconnects += 1
        end

        NATS.on_close do
          closes += 1
        end

        nc = NATS.connect({
          :servers => ['nats://secret:deadbeef@127.0.0.1:4445'],
            :tls => {
              :ssl_version => :TLSv1_2
            }
          }) do
          connects += 1
        end
        nc.subscribe("hello")
        nc.flush
      end
      expect(errors.count).to eql(1)
      expect(errors.first).to be_a(NATS::ConnectError)
      expect(connects).to eql(0)
      expect(closes).to eql(1)
      expect(disconnects).to eql(0)
    end

    it 'should connect securely to server and authorize' do
      errors = []
      messages = []
      connects = 0
      reconnects = 0
      disconnects = 0
      closes = 0

      with_em_timeout(10) do
        NATS.on_error do |e|
          errors << e
        end

        NATS.on_disconnect do |e|
          disconnects += 1
        end

        NATS.on_reconnect do
          reconnects += 1
        end

        NATS.on_close do
          closes += 1
        end

        options = {
          :servers => ['nats://secret:deadbeef@127.0.0.1:4445'],
          :max_reconnect_attempts => 1,
          :dont_randomize_servers => true,
          :tls => {
            :ssl_version => :TLSv1_2,
            :protocols => [:tlsv1_2],
            :private_key_file => './spec/configs/certs/key.pem',
            :cert_chain_file  => './spec/configs/certs/server.pem',
            :verify_peer      => false
          }
        }

        nc = NATS.connect(options) do |conn|
          expect(conn.connected_server).to eql(URI.parse('nats://secret:deadbeef@127.0.0.1:4445'))

          connects += 1
        end

        nc.subscribe("hello") do |msg|
          messages << msg
        end
        nc.flush do
          nc.publish("hello", "world") do
            nc.unsubscribe("hello")
            nc.close
          end
        end
      end
      expect(errors.count).to eql(0)
      expect(messages.count).to eql(1)
      expect(disconnects).to eql(0)
      expect(closes).to eql(1)
    end

    it 'should connect securely with default TLS and protocols options' do
      errors      = []
      messages    = []
      connects    = 0
      reconnects  = 0
      disconnects = 0
      closes      = 0

      with_em_timeout(10) do |future|

        NATS.on_error do |e|
          errors << e
        end

        NATS.on_disconnect do |e|
          disconnects += 1
        end

        NATS.on_reconnect do
          reconnects += 1
        end

        NATS.on_close do
          closes += 1
        end

        options = {
          :servers => ['nats://secret:deadbeef@127.0.0.1:4445'],
          :max_reconnect_attempts => 1,
          :dont_randomize_servers => true,
          :tls => {
            :private_key_file => './spec/configs/certs/key.pem',
            :cert_chain_file  => './spec/configs/certs/server.pem',
          }
        }

        nc = NATS.connect(options) do |conn|
          expect(conn.connected_server).to eql(URI.parse('nats://secret:deadbeef@127.0.0.1:4445'))
          connects += 1
        end

        nc.subscribe("hello") do |msg|
          messages << msg
        end
        nc.flush do
          nc.publish("hello", "world") do
            nc.unsubscribe("hello")
            nc.close
          end
        end
      end
      expect(messages.count).to eql(1)
      expect(errors.count).to eql(0)
      expect(closes).to eql(1)
      expect(reconnects).to eql(0)
      expect(disconnects).to eql(0)
    end
  end

  context 'when server requires TLS, certificates, authentication and client enables verify peer' do

    before(:each) do
      @tls_verify_auth = NatsServerControl.new("nats://127.0.0.1:4445", '/tmp/test-nats-4445.pid', "-c ./spec/configs/tlsverify.conf")
      @tls_verify_auth.start_server
    end

    after(:each) do
      @tls_verify_auth.kill_server
    end

    it 'should fail to connect if CA is not given' do
      errors = []
      connects = 0
      reconnects = 0
      disconnects = 0
      closes = 0

      with_em_timeout(3) do
        nc = nil
        NATS.on_error do |e|
          errors << e
        end

        NATS.on_disconnect do |e|
          disconnects += 1
        end

        NATS.on_reconnect do
          reconnects += 1
        end

        NATS.on_close do
          closes += 1
        end

        options = {
          :servers => ['nats://secret:deadbeef@127.0.0.1:4445'],
          :max_reconnect_attempts => 1,
          :dont_randomize_servers => true,
          :tls => {
            :ssl_version => :TLSv1_2,
            :protocols => [:tlsv1_2],
            :private_key_file => './spec/configs/certs/key.pem',
            :cert_chain_file  => './spec/configs/certs/server.pem',
            :verify_peer      => true
          }
        }
        expect do
          nc = NATS.connect(options) do |conn|
            connects += 1
          end
        end.to raise_error
      end
      expect(errors.count).to eql(0)
      expect(disconnects).to eql(0)
      expect(closes).to eql(0)
    end

    it 'should fail to connect if CA is not readable' do
      errors = []
      connects = 0
      reconnects = 0
      disconnects = 0
      closes = 0

      with_em_timeout(3) do
        nc = nil
        NATS.on_error do |e|
          errors << e
        end

        NATS.on_disconnect do |e|
          disconnects += 1
        end

        NATS.on_reconnect do
          reconnects += 1
        end

        NATS.on_close do
          closes += 1
        end

        options = {
          :servers => ['nats://secret:deadbeef@127.0.0.1:4445'],
          :max_reconnect_attempts => 1,
          :dont_randomize_servers => true,
          :tls => {
            :ssl_version => :TLSv1_2,
            :protocols => [:tlsv1_2],
            :private_key_file => './spec/configs/certs/key.pem',
            :cert_chain_file  => './spec/configs/certs/server.pem',
            :ca_file => './spec/configs/certs/does-not-exists.pem',
            :verify_peer      => true
          }
        }
        expect do
          nc = NATS.connect(options) do |conn|
            connects += 1
          end
        end.to raise_error
      end

      # No error here since it fails synchronously
      expect(errors.count).to eql(0)
      expect(disconnects).to eql(0)
      expect(closes).to eql(0)
    end

    it 'should connect securely to server and authorize' do
      errors = []
      messages = []
      connects = 0
      reconnects = 0
      disconnects = 0
      closes = 0

      with_em_timeout(10) do |future|
        NATS.on_error do |e|
          errors << e
        end

        NATS.on_disconnect do |e|
          disconnects += 1
        end

        NATS.on_reconnect do
          reconnects += 1
        end

        NATS.on_close do
          closes += 1
        end

        options = {
          :servers => ['nats://secret:deadbeef@127.0.0.1:4445'],
          :max_reconnect_attempts => 1,
          :dont_randomize_servers => true,
          :tls => {
            :ssl_version => :TLSv1_2,
            :protocols => [:tlsv1_2],
            :private_key_file => './spec/configs/certs/key.pem',
            :cert_chain_file  => './spec/configs/certs/server.pem',
            :ca_file => './spec/configs/certs/ca.pem',
            :verify_peer => true
          }
        }

        nc = NATS.connect(options) do |conn|
          expect(conn.connected_server).to eql(URI.parse('nats://secret:deadbeef@127.0.0.1:4445'))

          connects += 1
        end

        nc.subscribe("hello") do |msg|
          messages << msg
        end
        nc.flush do
          nc.publish("hello", "world") do
            nc.unsubscribe("hello")
            nc.close
            future.resume(nc)
          end
        end
      end
      expect(errors.count).to eql(0)
      expect(messages.count).to eql(1)
      expect(disconnects).to eql(0)
      expect(closes).to eql(1)
    end

    it 'should give up connecting securely to server if cannot verify peer' do
      errors = []
      messages = []
      connects = 0
      reconnects = 0
      disconnects = 0
      closes = 0

      with_em_timeout(10) do |future|
        NATS.on_error do |e|
          errors << e
        end

        NATS.on_disconnect do |e|
          disconnects += 1
        end

        NATS.on_reconnect do
          reconnects += 1
        end

        NATS.on_close do
          closes += 1
        end

        options = {
          :servers => ['nats://secret:deadbeef@127.0.0.1:4445'],
          :max_reconnect_attempts => 1,
          :dont_randomize_servers => true,
          :tls => {
            :ssl_version => :TLSv1_2,
            :protocols => [:tlsv1_2],
            :private_key_file => './spec/configs/certs/key.pem',
            :cert_chain_file  => './spec/configs/certs/server.pem',
            :ca_file => './spec/configs/certs/bad-ca.pem',
            :verify_peer => true
          }
        }

        nc = NATS.connect(options) do |conn|
          expect(conn.connected_server).to eql(URI.parse('nats://secret:deadbeef@127.0.0.1:4445'))

          connects += 1
        end

        nc.subscribe("hello") do |msg|
          messages << msg
        end
        nc.flush do
          nc.publish("hello", "world") do
            nc.unsubscribe("hello")
            nc.close
            future.resume(nc)
          end
        end
      end
      expect(errors.count).to eql(2)
      expect(errors.first).to be_a(NATS::ConnectError)
      expect(errors.last).to be_a(NATS::ConnectError)
      expect(messages.count).to eql(0)
      expect(disconnects).to eql(0)
      expect(closes).to eql(1)
    end

    it 'should connect securely with default TLS and protocols options and assume verify if CA given' do
      errors      = []
      messages    = []
      connects    = 0
      reconnects  = 0
      disconnects = 0
      closes      = 0

      with_em_timeout(10) do |future|

        NATS.on_error do |e|
          errors << e
        end

        NATS.on_disconnect do |e|
          disconnects += 1
        end

        NATS.on_reconnect do
          reconnects += 1
        end

        NATS.on_close do
          closes += 1
        end

        options = {
          :servers => ['nats://secret:deadbeef@127.0.0.1:4445'],
          :max_reconnect_attempts => 1,
          :dont_randomize_servers => true,
          :tls => {
            :private_key_file => './spec/configs/certs/key.pem',
            :cert_chain_file  => './spec/configs/certs/server.pem',
            :ca_file => './spec/configs/certs/ca.pem'
          }
        }

        nc = NATS.connect(options) do |conn|
          expect(conn.connected_server).to eql(URI.parse('nats://secret:deadbeef@127.0.0.1:4445'))
          connects += 1
        end

        nc.subscribe("hello") do |msg|
          messages << msg
        end
        nc.flush do
          nc.publish("hello", "world") do
            nc.unsubscribe("hello")
            nc.close
          end
        end
      end
      expect(messages.count).to eql(1)
      expect(errors.count).to eql(0)
      expect(closes).to eql(1)
      expect(reconnects).to eql(0)
      expect(disconnects).to eql(0)
    end
  end
end
