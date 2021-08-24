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

    def initialize(conn, params={})
      @nc = conn
      @opts = params
      @jsm = JSM.new(conn, params)
    end

    # publish produces a message for JetStream.
    def publish(subject, payload=EMPTY_MSG, params={})
      params[:timeout] ||= 0.5
      if params[:stream]
        params[:header] ||= {}
        params[:header][EXPECTED_STREAM_HDR] = params[:stream]
      end
      msg = NATS::Msg.new(subject: subject, data: payload, header: params[:header])

      begin
        resp = @nc.request_msg(msg, params)
        result = JSON.parse(resp.data, symbolize_names: true)
      rescue ::NATS::IO::NoRespondersError
        raise JetStream::NoStreamResponseError.new("nats: no response from stream")
      end
      raise JetStream::APIError.new(result[:error]) if result[:error]

      result
    end

    ####################################
    #                                  #
    # JetStream Configuration Options  #
    #                                  #
    ####################################

    DEFAULT_JS_API_PREFIX      = "$JS.API"
    NO_MSGS_STATUS             = "404"
    CTRL_MSG_STATUS            = "100"
    MSG_ID_HDR                 = "Nats-Msg-Id"
    EXPECTED_STREAM_HDR        = "Nats-Expected-Stream"
    EXPECTED_LAST_SEQ_HDR      = "Nats-Expected-Last-Sequence"
    EXPECTED_LAST_SUBJ_SEQ_HDR = "Nats-Expected-Last-Subject-Sequence"
    EXPECTED_LAST_MSG_ID_HDR   = "Nats-Expected-Last-Msg-Id"
    LAST_CONSUMER_SEQ_HDR      = "Nats-Last-Consumer"
    LAST_STREAM_SEQ_HDR        = "Nats-Last-Stream"

    # AckPolicy
    AckExplicit = "explicit"
    AckAll = "all"
    AckNone = "none"

    class JSM
      def initialize(conn=nil, prefix=DEFAULT_JS_API_PREFIX)
        @nc = conn
        @prefix = prefix
      end
    end
    private_constant :JSM

    #####################
    #                   #
    # JetStream Errors  #
    #                   #
    #####################

    class Error < StandardError; end

    # When there is a no responders error on a publish.
    class NoStreamResponseError < Error; end

    class APIError < Error
      attr_reader :code, :err_code, :description, :stream, :seq

      def initialize(params={})
        @code = params[:code]
        @err_code = params[:err_code]
        @description = params[:description]
        @stream = params[:stream]
        @seq = params[:seq]
      end

      def inspect
        "#<NATS::JetStream::APIError(code: #{@code}, err_code: #{@err_code}, description: '#{@description}')>"
      end
    end
  end
end
