require 'spec_helper'

describe 'error on client' do

  context 'NATS::ServerError' do

    it 'should show the contents of $1 when matched UNKNOWN' do
       EchoServer.start{
         EM.reactor_running?.should be_truthy
         NATS.on_error{|e|
           # We use a CONNECT request to match Unknown Protcol
           #e.to_s.should match(/\AUnknown Protocol: CONNECT\s+.*/i)
           e.to_s.should match(/\AUnknown Protocol: CONNECT.+/i)

           NATS.stop;
           EchoServer.stop
         }

         # Send CONNECT request to the server.
         NATS.start(:uri => EchoServer::ECHO_SERVER)
       }

       NATS.connected?.should be_falsey
       EM.reactor_running?.should be_falsey
    end
  end
end
