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
    ROLLUP = "Nats-Rollup"

    def initialize(opts={})
      @name = opts[:name]
      @stream = opts[:stream]
      @pre = opts[:pre]
      @js = opts[:js]
      @direct = opts[:direct]
    end

    class Error < NATS::Error; end

    # When a key is not found.
    class KeyNotFoundError < Error
      attr_reader :entry, :op
      def initialize(params={})
        @entry = params[:entry]
        @op = params[:op]
        @message = params[:message]
      end

      def to_s
        msg = "nats: key not found"
        msg = "#{msg}: #{@message}" if @message
        msg
      end
    end

    # When a key is not found because it was deleted.
    class KeyDeletedError < KeyNotFoundError
      def to_s
        "nats: key was deleted"
      end
    end

    # When there was no bucket present.
    class BucketNotFoundError < Error; end

    # When it is an invalid bucket.
    class BadBucketError < Error; end

    # When the result is an unexpected sequence.
    class KeyWrongLastSequenceError < Error
      def initialize(msg)
        @msg = msg
      end
      def to_s
        "nats: #{@msg}"
      end
    end

    # get returns the latest value for the key.
    def get(key, params={})
      entry = nil
      begin
        entry = _get(key, params)
      rescue KeyDeletedError
        raise KeyNotFoundError
      end

      entry
    end

    def _get(key, params={})
      msg = nil
      subject = "#{@pre}#{key}"

      if params[:revision]
        msg = @js.get_msg(@stream,
                          seq: params[:revision],
                          direct: @direct)
      else
        msg = @js.get_msg(@stream,
                          subject: subject,
                          seq: params[:revision],
                          direct: @direct)
      end

      entry = Entry.new(bucket: @name, key: key, value: msg.data, revision: msg.seq)

      if subject != msg.subject
        raise KeyNotFoundError.new(
          entry: entry,
          message: "expected '#{subject}', but got '#{msg.subject}'"
        )
      end

      if not msg.headers.nil?
        op = msg.headers[KV_OP]
        if op == KV_DEL or op == KV_PURGE
          raise KeyDeletedError.new(entry: entry, op: op)
        end
      end

      entry
    rescue NATS::JetStream::Error::NotFound
      raise KeyNotFoundError
    end
    private :_get

    # put will place the new value for the key into the store
    # and return the revision number.
    def put(key, value)
      ack = @js.publish("#{@pre}#{key}", value)
      ack.seq
    end

    # create will add the key/value pair iff it does not exist.
    def create(key, value)
      pa = nil
      begin
        pa = update(key, value, last: 0)
      rescue KeyWrongLastSequenceError => err
        # In case of attempting to recreate an already deleted key,
        # the client would get a KeyWrongLastSequenceError.  When this happens,
        # it is needed to fetch latest revision number and attempt to update.
        begin
          # NOTE: This reimplements the following behavior from Go client.
          #
          #   Since we have tombstones for DEL ops for watchers, this could be from that
          #   so we need to double check.
          #
          _get(key)

          # No exception so not a deleted key, so reraise the original KeyWrongLastSequenceError.
          # If it was deleted then the error exception will contain metadata
          # to recreate using the last revision.
          raise err
        rescue KeyDeletedError => err
          pa = update(key, value, last: err.entry.revision)
        end
      end

      pa
    end

    EXPECTED_LAST_SUBJECT_SEQUENCE = "Nats-Expected-Last-Subject-Sequence"

    # update will update the value iff the latest revision matches.
    def update(key, value, params={})
      hdrs = {}
      last = (params[:last] ||= 0)
      hdrs[EXPECTED_LAST_SUBJECT_SEQUENCE] = last.to_s
      ack = nil
      begin
        ack = @js.publish("#{@pre}#{key}", value, header: hdrs)
      rescue NATS::JetStream::Error::APIError => err
        if err.err_code == 10071
          raise KeyWrongLastSequenceError.new(err.description)
        else
          raise err
        end
      end

      ack.seq
    end

    # delete will place a delete marker and remove all previous revisions.
    def delete(key, params={})
      hdrs = {}
      hdrs[KV_OP] = KV_DEL
      last = (params[:last] ||= 0)
      if last > 0
        hdrs[EXPECTED_LAST_SUBJECT_SEQUENCE] = last.to_s
      end
      ack = @js.publish("#{@pre}#{key}", header: hdrs)

      ack.seq
    end

    # purge will remove the key and all revisions.
    def purge(key)
      hdrs = {}
      hdrs[KV_OP] = KV_PURGE
      hdrs[ROLLUP] = MSG_ROLLUP_SUBJECT
      @js.publish("#{@pre}#{key}", header: hdrs)
    end

    # status retrieves the status and configuration of a bucket.
    def status
      info = @js.stream_info(@stream)
      BucketStatus.new(info, @name)
    end

    Entry = Struct.new(:bucket, :key, :value, :revision, :delta, :created, :operation, keyword_init: true) do
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
        @nfo.config.max_age / ::NATS::NANOSECONDS
      end
    end

    module API
      KeyValueConfig = Struct.new(
          :bucket,
          :description,
          :max_value_size,
          :history,
          :ttl,
          :max_bytes,
          :storage,
          :replicas,
          :placement,
          :republish,
          :direct,
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
          direct: si.config.allow_direct
        )
      end

      def create_key_value(config)
        config = if not config.is_a?(KeyValue::API::KeyValueConfig)
                   KeyValue::API::KeyValueConfig.new(config)
                 else
                   config
                 end
        config.history ||= 1
        config.replicas ||= 1
        duplicate_window = 2 * 60 # 2 minutes
        if config.ttl
          if config.ttl < duplicate_window
            duplicate_window = config.ttl
          end
          config.ttl = config.ttl * ::NATS::NANOSECONDS
        end

        stream = JetStream::API::StreamConfig.new(
          name: "KV_#{config.bucket}",
          description: config.description,
          subjects: ["$KV.#{config.bucket}.>"],
          allow_direct: config.direct,
          allow_rollup_hdrs: true,
          deny_delete: true,
          discard: "new",
          duplicate_window: duplicate_window * ::NATS::NANOSECONDS,
          max_age: config.ttl,
          max_bytes: config.max_bytes,
          max_consumers: -1,
          max_msg_size: config.max_value_size,
          max_msgs: -1,
          max_msgs_per_subject: config.history,
          num_replicas: config.replicas,
          storage: config.storage,
          republish: config.republish,
        )

        si = add_stream(stream)
        KeyValue.new(
          name: config.bucket,
          stream: stream.name,
          pre: "$KV.#{config.bucket}.",
          js: self,
          direct: si.config.allow_direct
        )
      end

      def delete_key_value(bucket)
        delete_stream("KV_#{bucket}")
      end
    end
  end
end
