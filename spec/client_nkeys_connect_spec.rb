require 'spec_helper'

describe 'Client - NATS v2 Auth' do

  context 'with NKEYS and JWT' do
    before(:each) do
      config_opts = {
        'pid_file'      => '/tmp/nats_nkeys_jwt.pid',
        'host'          => '127.0.0.1',
        'port'          => 4722,
      }
      @s = NatsServerControl.init_with_config_from_string(%Q(
        authorization {
          timeout: 2
        }

        port = #{config_opts['port']}
        operator = "./spec/configs/nkeys/op.jwt"

        # This is for account resolution.
        resolver = MEMORY

         # This is a map that can preload keys:jwts into a memory resolver.
         resolver_preload = {
           # foo
           AD7SEANS6BCBF6FHIB7SQ3UGJVPW53BXOALP75YXJBBXQL7EAFB6NJNA : "eyJ0eXAiOiJqd3QiLCJhbGciOiJlZDI1NTE5In0.eyJqdGkiOiIyUDNHU1BFSk9DNlVZNE5aM05DNzVQVFJIV1pVRFhPV1pLR0NLUDVPNjJYSlZESVEzQ0ZRIiwiaWF0IjoxNTUzODQwNjE1LCJpc3MiOiJPRFdJSUU3SjdOT1M3M1dWQk5WWTdIQ1dYVTRXWFdEQlNDVjRWSUtNNVk0TFhUT1Q1U1FQT0xXTCIsIm5hbWUiOiJmb28iLCJzdWIiOiJBRDdTRUFOUzZCQ0JGNkZISUI3U1EzVUdKVlBXNTNCWE9BTFA3NVlYSkJCWFFMN0VBRkI2TkpOQSIsInR5cGUiOiJhY2NvdW50IiwibmF0cyI6eyJsaW1pdHMiOnsic3VicyI6LTEsImNvbm4iOi0xLCJpbXBvcnRzIjotMSwiZXhwb3J0cyI6LTEsImRhdGEiOi0xLCJwYXlsb2FkIjotMSwid2lsZGNhcmRzIjp0cnVlfX19.COiKg5EFK4Gb2gA7vtKHQK7vjMEUx-RMWYuN-Bg-uVOFs9GLwW7Dxc4TcN-poBGBEkwKnleiA9SjYO3y4-AqBQ"

           # bar
           AAXPTP32BD73YW3ACUY6DPXKWBSUW4VEZNE3LD4FUOFDP6KDU43PQVU2 : "eyJ0eXAiOiJqd3QiLCJhbGciOiJlZDI1NTE5In0.eyJqdGkiOiJPQ1dUQkRQTzVETjRSV0lFNEtJQ1BQWkszUEhHV0dQUVFKNFVET1pQSTVaRzJQUzZKVkpBIiwiaWF0IjoxNTUzODQwNjE5LCJpc3MiOiJPRFdJSUU3SjdOT1M3M1dWQk5WWTdIQ1dYVTRXWFdEQlNDVjRWSUtNNVk0TFhUT1Q1U1FQT0xXTCIsIm5hbWUiOiJiYXIiLCJzdWIiOiJBQVhQVFAzMkJENzNZVzNBQ1VZNkRQWEtXQlNVVzRWRVpORTNMRDRGVU9GRFA2S0RVNDNQUVZVMiIsInR5cGUiOiJhY2NvdW50IiwibmF0cyI6eyJsaW1pdHMiOnsic3VicyI6LTEsImNvbm4iOi0xLCJpbXBvcnRzIjotMSwiZXhwb3J0cyI6LTEsImRhdGEiOi0xLCJwYXlsb2FkIjotMSwid2lsZGNhcmRzIjp0cnVlfX19.KY2fBvYyNCA0dYS7I6_rETGHT4YGkWZSh03XhXxwAvJ8XCfKlVJRY82U-0ERg01SFtPTZ-6BYu-sty1E67ioDA"
         }
      ), config_opts)
      @s.start_server(true)
    end

    after(:each) do
      @s.kill_server
    end

    it 'should connect to server and publish messages' do
      mon = Monitor.new
      done = mon.new_cond

      errors = []
      msgs = []
      nats = NATS::IO::Client.new
      nats.on_error do |e|
        errors << e
      end
      nats.connect(servers: ['nats://127.0.0.1:4722'],
                   reconnect: false,
                   user_credentials: "./spec/configs/nkeys/foo-user.creds")
      nats.subscribe("hello") do |msg|
        msgs << msg
        done.signal
      end
      nats.flush
      nats.publish("hello", 'world')

      mon.synchronize do
        done.wait(1)
      end
      nats.close
      expect(msgs.count).to eql(1)
    end

    it 'should support user supplied credential callbacks' do
      mon = Monitor.new
      done = mon.new_cond

      errors = []
      msgs = []
      nats = NATS::IO::Client.new
      nats.on_error do |e|
        errors << e
      end

      user_sig_called = false
      sig_cb = proc { |nonce|
        user_sig_called = true
        nats.send(:signature_cb_for_creds_file, "./spec/configs/nkeys/foo-user.creds").call(nonce)
      }

      user_jwt_called = false
      jwt_cb = proc {
        user_jwt_called = true
        nats.send(:jwt_cb_for_creds_file, "./spec/configs/nkeys/foo-user.creds").call()
      }

      nats.connect(servers: ['nats://127.0.0.1:4722'],
                   reconnect: false,
                   user_signature_cb: sig_cb,
                   user_jwt_cb: jwt_cb)

      expect(user_sig_called).to be(true)
      expect(user_jwt_called).to be(true)

      nats.subscribe("hello") do |msg|
        msgs << msg
        done.signal
      end
      nats.flush
      nats.publish("hello", 'world')

      mon.synchronize do
        done.wait(1)
      end
      nats.close
      expect(msgs.count).to eql(1)
    end

    it 'should fail with auth error if no user credentials present' do
      mon = Monitor.new
      done = mon.new_cond

      errors = []
      msgs = []
      nats = NATS::IO::Client.new
      nats.on_error do |e|
        errors << e
      end

      expect do
        nats.connect(servers: ['nats://127.0.0.1:4722'],
                     reconnect: false)
      end.to raise_error(NATS::IO::AuthError)

      expect(errors.count).to eql(1)
    end
  end

  context 'with NKEYS only' do
    before(:each) do
      config_opts = {
        'pid_file'      => '/tmp/nats_nkeys.pid',
        'host'          => '127.0.0.1',
        'port'          => 4723,
      }
      @s = NatsServerControl.init_with_config_from_string(%Q(
        authorization {
          timeout: 2
        }

        port = #{config_opts['port']}

        accounts {
          acme {
            users [
              {
                 nkey = "UCK5N7N66OBOINFXAYC2ACJQYFSOD4VYNU6APEJTAVFZB2SVHLKGEW7L",
                 permissions = {
                   subscribe = {
                     allow = ["hello", "_INBOX.>"]
                     deny = ["foo"]
                   }
                   publish = {
                     allow = ["hello", "_INBOX.>"]
                     deny = ["foo"]
                   }
                 }
              }
            ]
          }
        }
      ), config_opts)
      @s.start_server(true)
    end

    after(:each) do
      @s.kill_server
    end

    it 'should connect to the server and publish messages' do
      mon = Monitor.new
      done = mon.new_cond

      errors = []
      msgs = []
      nats = NATS::IO::Client.new
      nats.on_error do |e|
        errors << e
      end
      nats.connect(servers: ['nats://127.0.0.1:4723'],
                   reconnect: false,
                   nkeys_seed: "./spec/configs/nkeys/foo-user.nk")
      nats.subscribe("hello") do |msg|
        msgs << msg
        done.signal
      end
      nats.flush
      nats.publish("hello", 'world')

      mon.synchronize do
        done.wait(1)
      end
      nats.close
      expect(msgs.count).to eql(1)
    end

    it 'should support user supplied nkey callbacks' do
      mon = Monitor.new
      done = mon.new_cond

      errors = []
      msgs = []
      nats = NATS::IO::Client.new
      nats.on_error do |e|
        errors << e
      end

      user_nkey_called = false
      user_nkey_cb = proc {
        user_nkey_called = true
        nats.send(:nkey_cb_for_nkey_file, "./spec/configs/nkeys/foo-user.nk").call()
      }

      user_sig_called = false
      sig_cb = proc { |nonce|
        user_sig_called = true
        nats.send(:signature_cb_for_nkey_file, "./spec/configs/nkeys/foo-user.nk").call(nonce)
      }

      nats.connect(servers: ['nats://127.0.0.1:4723'],
                   reconnect: false,
                   user_nkey_cb: user_nkey_cb,
                   user_signature_cb: sig_cb)

      expect(user_sig_called).to be(true)
      expect(user_nkey_called).to be(true)

      nats.subscribe("hello") do |msg|
        msgs << msg
        done.signal
      end
      nats.flush
      nats.publish("hello", 'world')

      mon.synchronize do
        done.wait(1)
      end
      nats.close
      expect(msgs.count).to eql(1)
    end
  end
end
