require 'spec_helper'

describe 'Client - partial message behavior' do
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
      with_em_timeout(5) do |future|
        # First client connects, and will attempt to reconnects
        c1 = NATS.connect(:uri => @s.uri, :reconnect_time_wait => 0.25)
        wait_on_connections([c1]) do
          c1.subscribe('subject') do |msg|
            got_message = true
            expect(msg).to eql('complete message')
            future.resume
          end

          # Client receives partial message before server terminates.
          c1.receive_data("MSG subject 2 32\r\nincomplete")

          # Server restarts, disconnecting the first client.
          @s.kill_server
          @s.start_server

          # One more client connects and publishes a message.
          NATS.connect(:uri => @s.uri) do |c2|
            EM.add_timer(0.50) do
              expect(c1.connected_server).to eql(@s.uri)              
              expect(c2.connected_server).to eql(@s.uri)
              c1.flush { c2.publish('subject', 'complete message') }
            end
          end
        end
      end
    end.to_not raise_exception

    expect(got_message).to eql(true)
  end
end
