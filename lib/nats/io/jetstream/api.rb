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

require_relative 'errors'
require 'base64'
require 'time'

module NATS
  class JetStream
    # JetStream::API are the types used to interact with the JetStream API.
    module API
      # When the server responds with an error from the JetStream API.
      Error = ::NATS::JetStream::Error::APIError

      # SequenceInfo is a pair of consumer and stream sequence and last activity.
      # @!attribute consumer_seq
      #   @return [Integer] The consumer sequence.
      # @!attribute stream_seq
      #   @return [Integer] The stream sequence.
      SequenceInfo = Struct.new(:consumer_seq, :stream_seq, :last_active,
                                keyword_init: true) do
        def initialize(opts={})
          # Filter unrecognized fields and freeze.
          rem = opts.keys - members
          opts.delete_if { |k| rem.include?(k) }
          super(opts)
          freeze
        end
      end

      # ConsumerInfo is the current status of a JetStream consumer.
      #
      # @!attribute stream_name
      #   @return [String] name of the stream to which the consumer belongs.
      # @!attribute name
      #   @return [String] name of the consumer.
      # @!attribute created
      #   @return [String] time when the consumer was created.
      # @!attribute config
      #   @return [ConsumerConfig] consumer configuration.
      # @!attribute delivered
      #   @return [SequenceInfo]
      # @!attribute ack_floor
      #   @return [SequenceInfo]
      # @!attribute num_ack_pending
      #   @return [Integer]
      # @!attribute num_redelivered
      #   @return [Integer]
      # @!attribute num_waiting
      #   @return [Integer]
      # @!attribute num_pending
      #   @return [Integer]
      # @!attribute cluster
      #   @return [Hash]
      ConsumerInfo = Struct.new(:type, :stream_name, :name, :created,
                                :config, :delivered, :ack_floor,
                                :num_ack_pending, :num_redelivered, :num_waiting,
                                :num_pending, :cluster, :push_bound,
                                keyword_init: true) do
        def initialize(opts={})
          opts[:created] = Time.parse(opts[:created])
          opts[:ack_floor] = SequenceInfo.new(opts[:ack_floor])
          opts[:delivered] = SequenceInfo.new(opts[:delivered])
          opts[:config][:ack_wait] = opts[:config][:ack_wait] / ::NATS::NANOSECONDS
          opts[:config] = ConsumerConfig.new(opts[:config])
          opts.delete(:cluster)
          # Filter unrecognized fields just in case.
          rem = opts.keys - members
          opts.delete_if { |k| rem.include?(k) }
          super(opts)
          freeze
        end
      end

      # ConsumerConfig is the consumer configuration.
      #
      # @!attribute durable_name
      #   @return [String]
      # @!attribute deliver_policy
      #   @return [String]
      # @!attribute ack_policy
      #   @return [String]
      # @!attribute ack_wait
      #   @return [Integer]
      # @!attribute max_deliver
      #   @return [Integer]
      # @!attribute replay_policy
      #   @return [String]
      # @!attribute max_waiting
      #   @return [Integer]
      # @!attribute max_ack_pending
      #   @return [Integer]
      ConsumerConfig = Struct.new(:name, :durable_name, :description,
                                  :deliver_policy, :opt_start_seq, :opt_start_time,
                                  :ack_policy, :ack_wait, :max_deliver, :backoff,
                                  :filter_subject, :replay_policy, :rate_limit_bps,
                                  :sample_freq, :max_waiting, :max_ack_pending,
                                  :flow_control, :idle_heartbeat, :headers_only,

                                  # Pull based options
                                  :max_batch, :max_expires,
                                  # Push based consumers
                                  :deliver_subject, :deliver_group,
                                  # Ephemeral inactivity threshold
                                  :inactive_threshold,
                                  # Generally inherited by parent stream and other markers,
                                  # now can be configured directly.
                                  :num_replicas,
                                  # Force memory storage
                                  :mem_storage,
                                  keyword_init: true) do
        def initialize(opts={})
          # Filter unrecognized fields just in case.
          rem = opts.keys - members
          opts.delete_if { |k| rem.include?(k) }
          super(opts)
        end
      end

      # StreamConfig represents the configuration of a stream from JetStream.
      #
      # @!attribute type
      #   @return [String]
      # @!attribute config
      #   @return [Hash]
      # @!attribute created
      #   @return [String]
      # @!attribute state
      #   @return [StreamState]
      # @!attribute did_create
      #   @return [Boolean]
      # @!attribute name
      #   @return [String]
      # @!attribute subjects
      #   @return [Array]
      # @!attribute retention
      #   @return [String]
      # @!attribute max_consumers
      #   @return [Integer]
      # @!attribute max_msgs
      #   @return [Integer]
      # @!attribute max_bytes
      #   @return [Integer]
      # @!attribute max_age
      #   @return [Integer]
      # @!attribute max_msgs_per_subject
      #   @return [Integer]
      # @!attribute max_msg_size
      #   @return [Integer]
      # @!attribute discard
      #   @return [String]
      # @!attribute storage
      #   @return [String]
      # @!attribute num_replicas
      #   @return [Integer]
      # @!attribute duplicate_window
      #   @return [Integer]
      StreamConfig = Struct.new(
        :name,
        :description,
        :subjects,
        :retention,
        :max_consumers,
        :max_msgs,
        :max_bytes,
        :discard,
        :max_age,
        :max_msgs_per_subject,
        :max_msg_size,
        :storage,
        :num_replicas,
        :no_ack,
        :duplicate_window,
        :placement,
        :mirror,
        :sources,
        :sealed,
        :deny_delete,
        :deny_purge,
        :allow_rollup_hdrs,
        :republish,
        :allow_direct,
        :mirror_direct,
        keyword_init: true) do
        def initialize(opts={})
          # Filter unrecognized fields just in case.
          rem = opts.keys - members
          opts.delete_if { |k| rem.include?(k) }
          super(opts)
        end
      end

      # StreamInfo is the info about a stream from JetStream.
      #
      # @!attribute type
      #   @return [String]
      # @!attribute config
      #   @return [Hash]
      # @!attribute created
      #   @return [String]
      # @!attribute state
      #   @return [Hash]
      # @!attribute domain
      #   @return [String]
      StreamInfo = Struct.new(:type, :config, :created, :state, :domain,
                              keyword_init: true) do
        def initialize(opts={})
          opts[:config] = StreamConfig.new(opts[:config])
          opts[:state] = StreamState.new(opts[:state])
          opts[:created] = ::Time.parse(opts[:created])

          # Filter fields and freeze.
          rem = opts.keys - members
          opts.delete_if { |k| rem.include?(k) }
          super(opts)
          freeze
        end
      end

      # StreamState is the state of a stream.
      #
      # @!attribute messages
      #   @return [Integer]
      # @!attribute bytes
      #   @return [Integer]
      # @!attribute first_seq
      #   @return [Integer]
      # @!attribute last_seq
      #   @return [Integer]
      # @!attribute consumer_count
      #   @return [Integer]
      StreamState = Struct.new(:messages, :bytes, :first_seq, :first_ts,
                               :last_seq, :last_ts, :consumer_count,
                               keyword_init: true) do
        def initialize(opts={})
          rem = opts.keys - members
          opts.delete_if { |k| rem.include?(k) }
          super(opts)
        end
      end

      # StreamCreateResponse is the response from the JetStream $JS.API.STREAM.CREATE API.
      #
      # @!attribute type
      #   @return [String]
      # @!attribute config
      #   @return [StreamConfig]
      # @!attribute created
      #   @return [String]
      # @!attribute state
      #   @return [StreamState]
      # @!attribute did_create
      #   @return [Boolean]
      StreamCreateResponse = Struct.new(:type, :config, :created, :state, :did_create,
                                        keyword_init: true) do
        def initialize(opts={})
          rem = opts.keys - members
          opts.delete_if { |k| rem.include?(k) }
          opts[:config] = StreamConfig.new(opts[:config])
          opts[:state] = StreamState.new(opts[:state])
          super(opts)
          freeze
        end
      end

      RawStreamMsg = Struct.new(:subject, :seq, :data, :headers, keyword_init: true) do
        def initialize(opts)
          opts[:data] = Base64.decode64(opts[:data]) if opts[:data]
          if opts[:hdrs]
            header = Base64.decode64(opts[:hdrs])
            hdr = {}
            lines = header.lines
            lines.slice(1, header.size).each do |line|
              line.rstrip!
              next if line.empty?
              key, value = line.strip.split(/\s*:\s*/, 2)
              hdr[key] = value
            end
            opts[:headers] = hdr
          end

          # Filter out members not present.
          rem = opts.keys - members
          opts.delete_if { |k| rem.include?(k) }
          super(opts)
        end

        def sequence
          self.seq
        end
      end
    end
  end
end
