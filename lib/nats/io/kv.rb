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

module NATS
  class KeyValue
    KV_OP = "KV-Operation"
    KV_DEL = "DEL"
    KV_PURGE = "PURGE"
    MSG_ROLLUP_SUBJECT = "sub"
    MSG_ROLLUP_ALL = "all"

    def initialize(opts={})
      @name = opts[:name]
      @stream = opts[:stream]
      @pre = opts[:pre]
      @js = opts[:js]
    end

    # When a key is not found because it was deleted.
    class KeyDeletedError < NATS::Error; end

    # When there was no bucket present.
    class BucketNotFoundError < NATS::Error; end

    # When it is an invalid bucket.
    class BadBucketError < NATS::Error; end

    # get returns the latest value for the key.
    def get(key)
      msg = @js.get_last_msg(@stream, "#{@pre}#{key}")
      entry = Entry.new(bucket: @name, key: key, value: msg.data, revision: msg.seq)

      if not msg.headers.nil?
        op = msg.headers[KV_OP]
        raise KeyDeletedError.new("nats: key was deleted") if op == KV_DEL or op == KV_PURGE
      end

      entry
    end

    # put will place the new value for the key into the store
    # and return the revision number.
    def put(key, value)
      @js.publish("#{@pre}#{key}", value)
    end

    # delete will place a delete marker and remove all previous revisions.
    def delete(key)
      hdrs = {}
      hdrs[KV_OP] = KV_DEL
      @js.publish("#{@pre}#{key}", header: hdrs)
    end

    # status retrieves the status and configuration of a bucket.
    def status
      info = @js.stream_info(@stream)
      BucketStatus.new(info, @name)
    end

    Entry = Struct.new(:bucket, :key, :value, :revision, keyword_init: true) do
      def initialize(opts={})
        rem = opts.keys - members
        opts.delete_if { |k| rem.include?(k) }
        super(opts)
      end
    end

    class BucketStatus
      attr_reader :bucket

      def initialize(info, bucket)
        @nfo = info
        @bucket = bucket
      end

      def values
        @nfo.state.messages
      end

      def history
        @nfo.config.max_msgs_per_subject
      end

      def ttl
        @nfo.config.max_age / 1_000_000_000
      end
    end
    
    module API      
      KeyValueConfig = Struct.new(:bucket, :description, :max_value_size,
                                  :history, :ttl, :max_bytes, :storage, :replicas,
                                  keyword_init: true) do
        def initialize(opts={})
          rem = opts.keys - members
          opts.delete_if { |k| rem.include?(k) }
          super(opts)
        end
      end
    end

    module Manager
      def key_value(bucket)
        stream = "KV_#{bucket}"
        begin
          si = stream_info(stream)
        rescue NATS::JetStream::Error::NotFound
          raise BucketNotFoundError.new("nats: bucket not found")
        end
        if si.config.max_msgs_per_subject < 1
          raise BadBucketError.new("nats: bad bucket")
        end

        KeyValue.new(
          name: bucket,
          stream: stream,
          pre: "$KV.#{bucket}.",
          js: self,
        )
      end

      def create_key_value(config)
        config = if not config.is_a?(JetStream::API::StreamConfig)
                   KeyValue::API::KeyValueConfig.new(config)
                 else
                   config
                 end
        config.history ||= 1
        config.replicas ||= 1
        if config.ttl
          config.ttl = config.ttl * 1_000_000_000
        end

        stream = JetStream::API::StreamConfig.new(
          name: "KV_#{config.bucket}",
          subjects: ["$KV.#{config.bucket}.>"],
          max_msgs_per_subject: config.history,
          max_bytes: config.max_bytes,
          max_age: config.ttl,
          max_msg_size: config.max_value_size,
          storage: config.storage,
          num_replicas: config.replicas,
          allow_rollup_hdrs: true,
          deny_delete: true,
        )
        resp = add_stream(stream)

        KeyValue.new(
          name: config.bucket,
          stream: stream.name,
          pre: "$KV.#{config.bucket}.",
          js: self,
        )
      end

      def delete_key_value(bucket)
        delete_stream("KV_#{bucket}")
      end
    end
  end
end
