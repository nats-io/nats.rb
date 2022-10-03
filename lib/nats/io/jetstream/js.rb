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

require_relative 'js/config'
require_relative 'js/header'
require_relative 'js/status'
require_relative 'js/sub'

module NATS
  class JetStream
    # Misc internal functions to support JS API.
    # @private
    module JS
      DefaultAPIPrefix = ("$JS.API".freeze)

      class << self
        def next_req_to_json(next_req)
          req = {}
          req[:batch] = next_req[:batch]
          req[:expires] = next_req[:expires].to_i if next_req[:expires]
          req[:no_wait] = next_req[:no_wait] if next_req[:no_wait]
          req.to_json
        end

        def is_status_msg(msg)
          return (!msg.nil? and (!msg.header.nil? and msg.header[Header::Status]))
        end

        # check_503_error raises exception when a NATS::Msg has a 503 status header.
        # @param msg [NATS::Msg] The message with status headers.
        # @raise [NATS::JetStream::Error::ServiceUnavailable]
        def check_503_error(msg)
          return if msg.nil? or msg.header.nil?
          if msg.header[Header::Status] == Status::ServiceUnavailable
            raise ::NATS::JetStream::Error::ServiceUnavailable
          end
        end

        # from_msg takes a plain NATS::Msg and checks its headers to confirm
        # if it was an error:
        #
        # msg.header={"Status"=>"503"})
        # msg.header={"Status"=>"408", "Description"=>"Request Timeout"})
        #
        # @param msg [NATS::Msg] The message with status headers.
        # @return [NATS::JetStream::API::Error]
        def from_msg(msg)
          check_503_error(msg)
          code = msg.header[JS::Header::Status]
          desc = msg.header[JS::Header::Desc]
          return ::NATS::JetStream::API::Error.new({code: code, description: desc})
        end

        # from_error takes an API response that errored and maps the error
        # into a JetStream error type based on the status and error code.
        def from_error(err)
          return unless err
          case err[:code]
          when 503
            ::NATS::JetStream::Error::ServiceUnavailable.new(err)
          when 500
            ::NATS::JetStream::Error::ServerError.new(err)
          when 404
            case err[:err_code]
            when 10059
              ::NATS::JetStream::Error::StreamNotFound.new(err)
            when 10014
              ::NATS::JetStream::Error::ConsumerNotFound.new(err)
            else
              ::NATS::JetStream::Error::NotFound.new(err)
            end
          when 400
            ::NATS::JetStream::Error::BadRequest.new(err)
          else
            ::NATS::JetStream::API::Error.new(err)
          end
        end
      end
    end
    private_constant :JS
  end
end
