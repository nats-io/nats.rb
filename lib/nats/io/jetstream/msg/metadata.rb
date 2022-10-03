# Copyright 2021 The NATS Authors
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

require 'time'

module NATS
  class JetStream
    module Msg
      class Metadata
        attr_reader :sequence, :num_delivered, :num_pending, :timestamp, :stream, :consumer, :domain

        def initialize(opts)
          @sequence      = Ack::SequencePair.new(opts[Ack::StreamSeq].to_i, opts[Ack::ConsumerSeq].to_i)
          @domain        = opts[Ack::Domain]
          @num_delivered = opts[Ack::NumDelivered].to_i
          @num_pending   = opts[Ack::NumPending].to_i
          @timestamp     = Time.at((opts[Ack::Timestamp].to_i / 1_000_000_000.0))
          @stream        = opts[Ack::Stream]
          @consumer      = opts[Ack::Consumer]
          # TODO: Not exposed in Go client either right now.
          # account      = opts[Ack::AccHash]
        end
      end
    end
  end
end
