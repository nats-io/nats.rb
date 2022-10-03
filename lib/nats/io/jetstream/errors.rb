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
  class JetStream
    # Error is any error that may arise when interacting with JetStream.
    class Error < Error

      # When there is a NATS::IO::NoResponders error after making a publish request.
      class NoStreamResponse < Error; end

      # When an invalid durable or consumer name was attempted to be used.
      class InvalidDurableName < Error; end

      # When an ack not longer valid.
      class InvalidJSAck < Error; end

      # When an ack has already been acked.
      class MsgAlreadyAckd < Error; end

      # When the delivered message does not behave as a message delivered by JetStream,
      # for example when the ack reply has unrecognizable fields.
      class NotJSMessage < Error; end

      # When the stream name is invalid.
      class InvalidStreamName < Error; end

      # When the consumer name is invalid.
      class InvalidConsumerName < Error; end

      # When the server responds with an error from the JetStream API.
      class APIError < Error
        attr_reader :code, :err_code, :description, :stream, :seq

        def initialize(params={})
          @code = params[:code]
          @err_code = params[:err_code]
          @description = params[:description]
          @stream = params[:stream]
          @seq = params[:seq]
        end

        def to_s
          "#{@description} (status_code=#{@code}, err_code=#{@err_code})"
        end
      end

      # When JetStream is not currently available, this could be due to JetStream
      # not being enabled or temporarily unavailable due to a leader election when
      # running in cluster mode.
      # This condition is represented with a message that has 503 status code header.
      class ServiceUnavailable < APIError
        def initialize(params={})
          super(params)
          @code ||= 503
        end
      end

      # When there is a hard failure in the JetStream.
      # This condition is represented with a message that has 500 status code header.
      class ServerError < APIError
        def initialize(params={})
          super(params)
          @code ||= 500
        end
      end

      # When a JetStream object was not found.
      # This condition is represented with a message that has 404 status code header.
      class NotFound < APIError
        def initialize(params={})
          super(params)
          @code ||= 404
        end
      end

      # When the stream is not found.
      class StreamNotFound < NotFound; end

      # When the consumer or durable is not found by name.
      class ConsumerNotFound < NotFound; end

      # When the JetStream client makes an invalid request.
      # This condition is represented with a message that has 400 status code header.
      class BadRequest < APIError
        def initialize(params={})
          super(params)
          @code ||= 400
        end
      end
    end
  end
end
