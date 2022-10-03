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

module NATS
  class KeyValue
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
