require 'spec_helper'

describe 'Client - error on client' do

  context 'NATS::ServerError' do

    it 'should show the contents of $1 when matched UNKNOWN' do
      EchoServer.start {
        EM.reactor_running?.should be_truthy
        NATS.on_error{|e|
          # We use a CONNECT request to match Unknown Protocol
          #e.to_s.should match(/\AUnknown Protocol: CONNECT\s+.*/i)
          e.to_s.should match(/\AUnknown Protocol: CONNECT.+/i)

          NATS.stop
          EchoServer.stop
        }

        # Send CONNECT request to the server.
        NATS.start(:uri => EchoServer::URI)
      }

      expect(NATS.connected?).to be_falsey
      expect(EM.reactor_running?).to be_falsey
    end

    it 'should disconnect when pings outstanding over limit' do
      nc = nil
      errors = []
      closes = 0

      SilentServer.start {
        EM.reactor_running?.should be_truthy

        NATS.on_error do |e|
          errors << e
          NATS.stop
          EchoServer.stop
        end

        NATS.on_close do
          closes += 1
        end

        nc = NATS.connect({
          :servers => [SilentServer::URI],
          :max_outstanding_pings => 2,
          :ping_interval => 1,
          :reconnect => false
        })
        nc.should_receive(:queue_server_rt).exactly(2).times
      }

      expect(nc.connected?).to be_falsey
      expect(EM.reactor_running?).to be_falsey
      expect(errors.first).to be_a(NATS::ConnectError)
      expect(closes).to eql(1)
    end
  end
end
