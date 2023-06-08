# Copyright 2016-2021 The NATS Authors
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

module NATS

  # A Subscription represents interest in a given subject.
  # 
  # @example Create NATS subscription with callback.
  #   require 'nats/client'
  # 
  #   nc = NATS.connect("demo.nats.io")
  #   sub = nc.subscribe("foo") do |msg|
  #     puts "Received [#{msg.subject}]: #{}"
  #   end
  # 
  class Subscription
    include MonitorMixin

    attr_accessor :subject, :queue, :future, :callback, :response, :received, :max, :pending, :sid
    attr_accessor :pending_queue, :pending_size, :wait_for_msgs_cond, :concurrency_semaphore
    attr_accessor :pending_msgs_limit, :pending_bytes_limit
    attr_accessor :nc
    attr_accessor :jsi
    attr_accessor :closed

    def initialize(**opts)
      super() # required to initialize monitor
      @subject  = ''
      @queue    = nil
      @future   = nil
      @callback = nil
      @response = nil
      @received = 0
      @max      = nil
      @pending  = nil
      @sid      = nil
      @nc       = nil
      @closed   = nil

      # State from async subscriber messages delivery
      @pending_queue       = nil
      @pending_size        = 0
      @pending_msgs_limit  = nil
      @pending_bytes_limit = nil

      # Sync subscriber
      @wait_for_msgs_cond = nil

      # To limit number of concurrent messages being processed (1 to only allow sequential processing)
      @processing_concurrency = opts.fetch(:processing_concurrency, NATS::IO::DEFAULT_SINGLE_SUB_CONCURRENCY)
    end

    # Concurrency of message processing for a single subscription.
    # 1 means sequential processing
    # 2+ allow processed concurrently and possibly out of order.
    def processing_concurrency=(value)
      raise ArgumentError, "nats: subscription processing concurrency must be positive integer" unless value.positive?
      return if @processing_concurrency == value

      @processing_concurrency = value
      @concurrency_semaphore = Concurrent::Semaphore.new(value)
    end

    def concurrency_semaphore
      @concurrency_semaphore ||= Concurrent::Semaphore.new(@processing_concurrency)
    end

    # Auto unsubscribes the server by sending UNSUB command and throws away
    # subscription in case already present and has received enough messages.
    def unsubscribe(opt_max=nil)
      @nc.send(:unsubscribe, self, opt_max)
    end

    # next_msg blocks and waiting for the next message to be received.
    def next_msg(opts={})
      timeout = opts[:timeout] ||= 0.5
      synchronize do
        return @pending_queue.pop if not @pending_queue.empty?

        # Wait for a bit until getting a signal.
        MonotonicTime::with_nats_timeout(timeout) do
          wait_for_msgs_cond.wait(timeout)
        end

        if not @pending_queue.empty?
          return @pending_queue.pop
        else
          raise NATS::Timeout
        end
      end
    end

    def inspect
      "#<NATS::Subscription(subject: \"#{@subject}\", queue: \"#{@queue}\", sid: #{@sid})>"
    end

    def dispatch(msg)
      pending_queue << msg
      synchronize { self.pending_size += msg.data.size }

      # For async subscribers, send message for processing to the thread pool.
      enqueue_processing(@nc.subscription_executor) if callback

      # For sync subscribers, signal that there is a new message.
      wait_for_msgs_cond&.signal
    end

    def process(msg)
      return unless callback

      # Decrease pending size since consumed already
      synchronize { self.pending_size -= msg.data.size }

      nc.reloader.call do
        # Note: Keep some of the alternative arity versions to slightly
        # improve backwards compatibility.  Eventually fine to deprecate
        # since recommended version would be arity of 1 to get a NATS::Msg.
        case callback.arity
        when 0 then callback.call
        when 1 then callback.call(msg)
        when 2 then callback.call(msg.data, msg.reply)
        when 3 then callback.call(msg.data, msg.reply, msg.subject)
        else callback.call(msg.data, msg.reply, msg.subject, msg.header)
        end
      rescue => e
        synchronize { nc.send(:err_cb_call, nc, e, self) }
      end
    end

    # Send a message for its processing to a separate thread
    def enqueue_processing(executor)
      concurrency_semaphore.try_acquire || return # Previous message is being executed, let it finish and enqueue next one.
      executor.post do
        msg = pending_queue.pop(true)
        process(msg)
      rescue ThreadError # queue is empty
        concurrency_semaphore.release
      ensure
        concurrency_semaphore.release
        [concurrency_semaphore.available_permits, pending_queue.size].min.times do
          enqueue_processing(executor)
        end
      end
    end
  end
end
