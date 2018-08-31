require 'nats/client'

nc1 = nil
nc2 = nil
responses = []
inbox = NATS.create_inbox
["TERM", "INT"].each { |sig| trap(sig) {
    EM.stop
  }
}

subscribers = []
EM.run do
  5.times do |n|
    subscribers << NATS.connect(drain_timeout: 30, name: "client-#{n}") do |nc|
      nc.on_error { |err| puts "#{Time.now} - Error: #{err}" }
      nc.on_close { |err| puts "#{Time.now} - Connection drained and closed!" }
      puts "#{Time.now} - Started Connection #{n}..."

      nc.flush do
        nc.subscribe('foo', queue: "workers") do |msg, reply, sub|
          nc.publish(reply, "ACK1:#{msg}")
        end

        nc.subscribe('bar', queue: "workers") do |msg, reply, sub|
          nc.publish(reply, "ACK1:#{msg}")
        end

        nc.subscribe('quux', queue: "workers") do |msg, reply, sub|
          nc.publish(reply, "ACK1:#{msg}")
        end
      end
    end
  end

  pub_client = NATS.connect do |nc|
    EM.add_periodic_timer(0.001) do
      Fiber.new do
        response = nc.request("foo", "A")
        puts "Dropped request!!!" if response.nil?
      end.resume
    end

    EM.add_periodic_timer(0.001) do
      Fiber.new do
        response = nc.request("bar", "B")
        puts "Dropped request!!!" if response.nil?
        # puts "Response on 'bar' : #{response}"
      end.resume
    end

    EM.add_periodic_timer(0.001) do
      Fiber.new do
        response = nc.request("quux", "C")
        puts "Dropped request!!!" if response.nil?
      end.resume
    end
  end

  EM.add_timer(1) do
    # Drain is like stop but gracefully closes the connection.
    subs = subscribers[0..3]

    subs.each_with_index do |nc, i|
      if nc.draining?
        puts "Already draining... #{responses.count}"
        next
      end

      # Just using close will cause some requests to fail
      # nc.close

      # Drain is more graceful and allow clients to process requests
      # that have already been delivered by the server to the subscriber.
      puts "#{Time.now} - Start draining  #{nc.options[:name]}... (pending_data: #{nc.pending_data_size})"
      nc.drain do
        puts "#{Time.now} - Done draining #{nc.options[:name]}!"
      end
    end
  end
end
