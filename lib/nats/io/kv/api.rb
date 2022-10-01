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
  end
end
