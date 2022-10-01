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

require_relative '../errors'

module NATS
  class KeyValue
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
  end
end
