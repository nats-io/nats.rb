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
    # A JetStream::Manager can be used to make requests to the JetStream API.
    #
    # @example
    #   require 'nats/client'
    #
    #   nc = NATS.connect("demo.nats.io")
    #
    #   config = JetStream::API::StreamConfig.new()
    #   nc.jsm.add_stream(config)
    #
    #
    module Manager
      # add_stream creates a stream with a given config.
      # @param config [JetStream::API::StreamConfig] Configuration of the stream to create.
      # @param params [Hash] Options to customize API request.
      # @option params [Float] :timeout Time to wait for response.
      # @return [JetStream::API::StreamCreateResponse] The result of creating a Stream.
      def add_stream(config, params={})
        config = if not config.is_a?(JetStream::API::StreamConfig)
                   JetStream::API::StreamConfig.new(config)
                 else
                   config
                 end
        stream = config[:name]
        raise ArgumentError.new(":name is required to create streams") unless stream
        raise ArgumentError.new("Spaces, tabs, period (.), greater than (>) or asterisk (*) are prohibited in stream names") if stream =~ /(\s|\.|\>|\*)/
        req_subject = "#{@prefix}.STREAM.CREATE.#{stream}"

        cfg = config.to_h.compact
        result = api_request(req_subject, cfg.to_json, params)
        JetStream::API::StreamCreateResponse.new(result)
      end

      # stream_info retrieves the current status of a stream.
      # @param stream [String] Name of the stream.
      # @param params [Hash] Options to customize API request.
      # @option params [Float] :timeout Time to wait for response.
      # @return [JetStream::API::StreamInfo] The latest StreamInfo of the stream.
      def stream_info(stream, params={})
        raise JetStream::Error::InvalidStreamName.new("nats: invalid stream name") if stream.nil? or stream.empty?

        req_subject = "#{@prefix}.STREAM.INFO.#{stream}"
        result = api_request(req_subject, '', params)
        JetStream::API::StreamInfo.new(result)
      end

      # update_stream edits an existed stream with a given config.
      # @param config [JetStream::API::StreamConfig] Configuration of the stream to create.
      # @param params [Hash] Options to customize API request.
      # @option params [Float] :timeout Time to wait for response.
      # @return [JetStream::API::StreamCreateResponse] The result of creating a Stream.
      def update_stream(config, params={})
        config = if not config.is_a?(JetStream::API::StreamConfig)
                   JetStream::API::StreamConfig.new(config)
                 else
                   config
                 end
        stream = config[:name]
        raise ArgumentError.new(":name is required to create streams") unless stream
        raise ArgumentError.new("Spaces, tabs, period (.), greater than (>) or asterisk (*) are prohibited in stream names") if stream =~ /(\s|\.|\>|\*)/
        req_subject = "#{@prefix}.STREAM.UPDATE.#{stream}"
        cfg = config.to_h.compact
        result = api_request(req_subject, cfg.to_json, params)
        JetStream::API::StreamCreateResponse.new(result)
      end

      # delete_stream deletes a stream.
      # @param stream [String] Name of the stream.
      # @param params [Hash] Options to customize API request.
      # @option params [Float] :timeout Time to wait for response.
      # @return [Boolean]
      def delete_stream(stream, params={})
        raise JetStream::Error::InvalidStreamName.new("nats: invalid stream name") if stream.nil? or stream.empty?

        req_subject = "#{@prefix}.STREAM.DELETE.#{stream}"
        result = api_request(req_subject, '', params)
        result[:success]
      end

      # add_consumer creates a consumer with a given config.
      # @param stream [String] Name of the stream.
      # @param config [JetStream::API::ConsumerConfig] Configuration of the consumer to create.
      # @param params [Hash] Options to customize API request.
      # @option params [Float] :timeout Time to wait for response.
      # @return [JetStream::API::ConsumerInfo] The result of creating a Consumer.
      def add_consumer(stream, config, params={})
        raise JetStream::Error::InvalidStreamName.new("nats: invalid stream name") if stream.nil? or stream.empty?
        config = if not config.is_a?(JetStream::API::ConsumerConfig)
                   JetStream::API::ConsumerConfig.new(config)
                 else
                   config
                 end

        req_subject = case
                      when config[:name]
                        # NOTE: Only supported after nats-server v2.9.0
                        if config[:filter_subject] && config[:filter_subject] != ">"
                          "#{@prefix}.CONSUMER.CREATE.#{stream}.#{config[:name]}.#{config[:filter_subject]}"
                        else
                          "#{@prefix}.CONSUMER.CREATE.#{stream}.#{config[:name]}"
                        end
                      when config[:durable_name]
                        "#{@prefix}.CONSUMER.DURABLE.CREATE.#{stream}.#{config[:durable_name]}"
                      else
                        "#{@prefix}.CONSUMER.CREATE.#{stream}"
                      end

        config[:ack_policy] ||= JS::Config::AckExplicit
        # Check if have to normalize ack wait so that it is in nanoseconds for Go compat.
        if config[:ack_wait]
          raise ArgumentError.new("nats: invalid ack wait") unless config[:ack_wait].is_a?(Integer)
          config[:ack_wait] = config[:ack_wait] * ::NATS::NANOSECONDS
        end
        if config[:inactive_threshold]
          raise ArgumentError.new("nats: invalid inactive threshold") unless config[:inactive_threshold].is_a?(Integer)
          config[:inactive_threshold] = config[:inactive_threshold] * ::NATS::NANOSECONDS
        end

        cfg = config.to_h.compact
        req = {
          stream_name: stream,
          config: cfg
        }

        result = api_request(req_subject, req.to_json, params)
        JetStream::API::ConsumerInfo.new(result).freeze
      end

      # consumer_info retrieves the current status of a consumer.
      # @param stream [String] Name of the stream.
      # @param consumer [String] Name of the consumer.
      # @param params [Hash] Options to customize API request.
      # @option params [Float] :timeout Time to wait for response.
      # @return [JetStream::API::ConsumerInfo] The latest ConsumerInfo of the consumer.
      def consumer_info(stream, consumer, params={})
        raise JetStream::Error::InvalidStreamName.new("nats: invalid stream name") if stream.nil? or stream.empty?
        raise JetStream::Error::InvalidConsumerName.new("nats: invalid consumer name") if consumer.nil? or consumer.empty?

        req_subject = "#{@prefix}.CONSUMER.INFO.#{stream}.#{consumer}"
        result = api_request(req_subject, '', params)
        JetStream::API::ConsumerInfo.new(result)
      end

      # delete_consumer deletes a consumer.
      # @param stream [String] Name of the stream.
      # @param consumer [String] Name of the consumer.
      # @param params [Hash] Options to customize API request.
      # @option params [Float] :timeout Time to wait for response.
      # @return [Boolean]
      def delete_consumer(stream, consumer, params={})
        raise JetStream::Error::InvalidStreamName.new("nats: invalid stream name") if stream.nil? or stream.empty?
        raise JetStream::Error::InvalidConsumerName.new("nats: invalid consumer name") if consumer.nil? or consumer.empty?

        req_subject = "#{@prefix}.CONSUMER.DELETE.#{stream}.#{consumer}"
        result = api_request(req_subject, '', params)
        result[:success]
      end

      # find_stream_name_by_subject does a lookup for the stream to which
      # the subject belongs.
      # @param subject [String] The subject that belongs to a stream.
      # @param params [Hash] Options to customize API request.
      # @option params [Float] :timeout Time to wait for response.
      # @return [String] The name of the JetStream stream for the subject.
      def find_stream_name_by_subject(subject, params={})
        req_subject = "#{@prefix}.STREAM.NAMES"
        req = { subject: subject }
        result = api_request(req_subject, req.to_json, params)
        raise JetStream::Error::NotFound unless result[:streams]

        result[:streams].first
      end

      # get_msg retrieves a message from the stream.
      # @param stream_name [String] The stream_name.
      # @param params [Hash] Options to customize API request.
      # @option next [Boolean] Fetch the next message for a subject.
      # @option seq [Integer] Sequence number of a message.
      # @option subject [String] Subject of the message.
      # @option direct [Boolean] Use direct mode to for faster access (requires NATS v2.9.0)
      def get_msg(stream_name, params={})
        req = {}
        case
        when params[:next]
          req[:seq] = params[:seq]
          req[:next_by_subj] = params[:subject]
        when params[:seq]
          req[:seq] = params[:seq]
        when params[:subject]
          req[:last_by_subj] = params[:subject]
        end

        data = req.to_json
        if params[:direct]
          if params[:subject] and not params[:seq]
            # last_by_subject type request requires no payload.
            data = ''
            req_subject = "#{@prefix}.DIRECT.GET.#{stream_name}.#{params[:subject]}"
          else
            req_subject = "#{@prefix}.DIRECT.GET.#{stream_name}"
          end
        else
          req_subject = "#{@prefix}.STREAM.MSG.GET.#{stream_name}"
        end
        resp = api_request(req_subject, data, direct: params[:direct])
        msg = if params[:direct]
                _lift_msg_to_raw_msg(resp)
              else
                JetStream::API::RawStreamMsg.new(resp[:message])
              end

        msg
      end

      def get_last_msg(stream_name, subject, params={})
        params[:subject] = subject
        get_msg(stream_name, params)
      end

      def account_info
        api_request("#{@prefix}.INFO")
      end

      private

      def api_request(req_subject, req="", params={})
        params[:timeout] ||= @opts[:timeout]
        msg = begin
                @nc.request(req_subject, req, **params)
              rescue NATS::IO::NoRespondersError
                raise JetStream::Error::ServiceUnavailable
              end

        result = if params[:direct]
                   msg
                 else
                   JSON.parse(msg.data, symbolize_names: true)
                 end
        if result.is_a?(Hash) and result[:error]
          raise JS.from_error(result[:error])
        end

        result
      end

      def _lift_msg_to_raw_msg(msg)
        if msg.header and msg.header['Status']
          status = msg.header['Status']
          if status == '404'
            raise ::NATS::JetStream::Error::NotFound.new
          else
            raise JS.from_msg(msg)
          end
        end
        subject = msg.header['Nats-Subject']
        seq = msg.header['Nats-Sequence']
        raw_msg = JetStream::API::RawStreamMsg.new(
          subject: subject,
          seq: seq,
          headers: msg.header,
          )
        raw_msg.data = msg.data

        raw_msg
      end
    end
  end
end
