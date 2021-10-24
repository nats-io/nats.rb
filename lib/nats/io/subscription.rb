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
  class Subscription
    include MonitorMixin

    attr_accessor :subject, :queue, :future, :callback, :response, :received, :max, :pending, :sid
    attr_accessor :pending_queue, :pending_size, :wait_for_msgs_t, :wait_for_msgs_cond, :is_slow_consumer
    attr_accessor :pending_msgs_limit, :pending_bytes_limit
    attr_accessor :nc
    attr_accessor :jsi

    def initialize
      super # required to initialize monitor
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

      # State from async subscriber messages delivery
      @pending_queue       = nil
      @pending_size        = 0
      @pending_msgs_limit  = nil
      @pending_bytes_limit = nil
      @wait_for_msgs_t     = nil
      @is_slow_consumer    = false

      # Sync subscriber
      @wait_for_msgs_cond = nil
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
      end
    end

    def inspect
      "#<NATS::Subscription(subject: \"#{@subject}\", queue: \"#{@queue}\", sid: #{@sid})>"
    end
  end
end
