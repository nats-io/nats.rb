require 'spec_helper'

describe 'partial message behavior' do
  before do
    @s = NatsServerControl.new
    @s.start_server
  end

  after do
    @s.kill_server
  end

  it 'should not hold stale message data across a reconnect' do
    got_message = false
    expect do
      EM.run do
        timeout_em_on_failure(2)
        timeout_nats_on_failure(2)

        c1 = NATS.connect(:uri => @s.uri, :reconnect_time_wait => 0.25)
        wait_on_connections([c1]) do
          c1.subscribe('subject') do |msg|
            got_message = true
            msg.should eq('complete message')
            EM.stop
            NATS.stop
          end

          # Client receives partial message before server terminates
          c1.receive_data("MSG subject 2 32\r\nincomplete")

          @s.kill_server
          @s.start_server

          NATS.connect(:uri => @s.uri) do |c2|
            EM.add_timer(0.25) do
              c1.connected_server.should == @s.uri
              c1.flush { c2.publish('subject', 'complete message') }
            end
          end
        end
      end
    end.to_not raise_exception

    got_message.should be_truthy
  end
end
