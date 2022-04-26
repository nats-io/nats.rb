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
require_relative 'msg'
require_relative 'client'
require_relative 'errors'
require_relative 'kv'
require 'time'
require 'base64'

module NATS

  # JetStream returns a context with a similar API as the NATS::Client
  # but with enhanced functions to persist and consume messages from
  # the NATS JetStream engine.
  #
  # @example
  #   nc = NATS.connect("demo.nats.io")
  #   js = nc.jetstream()
  #
  class JetStream
    # Create a new JetStream context for a NATS connection.
    #
    # @param conn [NATS::Client]
    # @param params [Hash] Options to customize JetStream context.
    # @option params [String] :prefix JetStream API prefix to use for the requests.
    # @option params [String] :domain JetStream Domain to use for the requests.
    # @option params [Float] :timeout Default timeout to use for JS requests.
    def initialize(conn, params={})
      @nc = conn
      @prefix = if params[:prefix]
                  params[:prefix]
                elsif params[:domain]
                  "$JS.#{params[:domain]}.API"
                else
                  JS::DefaultAPIPrefix
                end
      @opts = params
      @opts[:timeout] ||= 5 # seconds
      params[:prefix] = @prefix

      # Include JetStream::Manager
      extend Manager
      extend KeyValue::Manager
    end

    # PubAck is the API response from a successfully published message.
    #
    # @!attribute [stream] stream
    #   @return [String] Name of the stream that processed the published message.
    # @!attribute [seq] seq
    #   @return [Fixnum] Sequence of the message in the stream.
    # @!attribute [duplicate] duplicate
    #   @return [Boolean] Indicates whether the published message is a duplicate.
    # @!attribute [domain] domain
    #   @return [String] JetStream Domain that processed the ack response.
    PubAck = Struct.new(:stream, :seq, :duplicate, :domain, keyword_init: true)

    # publish produces a message for JetStream.
    #
    # @param subject [String] The subject from a stream where the message will be sent.
    # @param payload [String] The payload of the message.
    # @param params [Hash] Options to customize the publish message request.
    # @option params [Float] :timeout Time to wait for an PubAck response or an error.
    # @option params [Hash] :header NATS Headers to use for the message.
    # @option params [String] :stream Expected Stream to which the message is being published.
    # @raise [NATS::Timeout] When it takes too long to receive an ack response.
    # @return [PubAck] The pub ack response.
    def publish(subject, payload="", **params)
      params[:timeout] ||= @opts[:timeout]
      if params[:stream]
        params[:header] ||= {}
        params[:header][JS::Header::ExpectedStream] = params[:stream]
      end

      # Send message with headers.
      msg = NATS::Msg.new(subject: subject,
                          data: payload,
                          header: params[:header])

      begin
        resp = @nc.request_msg(msg, **params)
        result = JSON.parse(resp.data, symbolize_names: true)
      rescue ::NATS::IO::NoRespondersError
        raise JetStream::Error::NoStreamResponse.new("nats: no response from stream")
      end
      raise JS.from_error(result[:error]) if result[:error]

      PubAck.new(result)
    end

    # subscribe binds or creates a push subscription to a JetStream pull consumer.
    #
    # @param subject [String] Subject from which the messages will be fetched.
    # @param params [Hash] Options to customize the PushSubscription.
    # @option params [String] :stream Name of the Stream to which the consumer belongs.
    # @option params [String] :consumer Name of the Consumer to which the PushSubscription will be bound.
    # @option params [String] :durable Consumer durable name from where the messages will be fetched.
    # @option params [Hash] :config Configuration for the consumer.
    # @return [NATS::JetStream::PushSubscription]
    def subscribe(subject, params={}, &cb)
      params[:consumer] ||= params[:durable]
      stream = params[:stream].nil? ? find_stream_name_by_subject(subject) : params[:stream]

      queue = params[:queue]
      durable = params[:durable]
      flow_control = params[:flow_control]
      manual_ack = params[:manual_ack]
      idle_heartbeat = params[:idle_heartbeat]
      flow_control = params[:flow_control]

      if queue
        if durable and durable != queue
          raise NATS::JetStream::Error.new("nats: cannot create queue subscription '#{queue}' to consumer '#{durable}'")
        else
          durable = queue
        end
      end

      cinfo = nil
      consumer_found = false
      should_create = false

      if not durable
        should_create = true
      else
        begin
          cinfo = consumer_info(stream, durable)
          config = cinfo.config
          consumer_found = true
          consumer = durable
        rescue NATS::JetStream::Error::NotFound
          should_create = true
          consumer_found = false
        end
      end

      if consumer_found
        if not config.deliver_group
          if queue
            raise NATS::JetStream::Error.new("nats: cannot create a queue subscription for a consumer without a deliver group")
          elsif cinfo.push_bound
            raise NATS::JetStream::Error.new("nats: consumer is already bound to a subscription")
          end
        else
          if not queue
            raise NATS::JetStream::Error.new("nats: cannot create a subscription for a consumer with a deliver group #{config.deliver_group}")
          elsif queue != config.deliver_group
            raise NATS::JetStream::Error.new("nats: cannot create a queue subscription #{queue} for a consumer with a deliver group #{config.deliver_group}")
          end
        end
      elsif should_create
        # Auto-create consumer if none found.
        if config.nil?
          # Defaults
          config = JetStream::API::ConsumerConfig.new({ack_policy: "explicit"})
        elsif config.is_a?(Hash)
          config = JetStream::API::ConsumerConfig.new(config)
        elsif !config.is_a?(JetStream::API::ConsumerConfig)
          raise NATS::JetStream::Error.new("nats: invalid ConsumerConfig")
        end

        config.durable_name = durable if not config.durable_name
        config.deliver_group = queue if not config.deliver_group

        # Create inbox for push consumer.
        deliver = @nc.new_inbox
        config.deliver_subject = deliver

        # Auto created consumers use the filter subject.
        config.filter_subject = subject

        # Heartbeats / FlowControl
        config.flow_control = flow_control
        if idle_heartbeat or config.idle_heartbeat
          idle_heartbeat = config.idle_heartbeat if config.idle_heartbeat
          idle_heartbeat = idle_heartbeat * 1_000_000_000
          config.idle_heartbeat = idle_heartbeat
        end

        # Auto create the consumer.
        cinfo = add_consumer(stream, config)
        consumer = cinfo.name
      end

      # Enable auto acking for async callbacks unless disabled.
      if cb and not manual_ack
        ocb = cb
        new_cb = proc do |msg|
          ocb.call(msg)
          msg.ack rescue JetStream::Error::MsgAlreadyAckd
        end
        cb = new_cb
      end
      sub = @nc.subscribe(config.deliver_subject, queue: config.deliver_group, &cb)
      sub.extend(PushSubscription)
      sub.jsi = JS::Sub.new(
        js: self,
        stream: stream,
        consumer: consumer,
      )
      sub
    end

    # pull_subscribe binds or creates a subscription to a JetStream pull consumer.
    #
    # @param subject [String] Subject from which the messages will be fetched.
    # @param durable [String] Consumer durable name from where the messages will be fetched.
    # @param params [Hash] Options to customize the PullSubscription.
    # @option params [String] :stream Name of the Stream to which the consumer belongs.
    # @option params [String] :consumer Name of the Consumer to which the PullSubscription will be bound.
    # @option params [Hash] :config Configuration for the consumer.
    # @return [NATS::JetStream::PullSubscription]
    def pull_subscribe(subject, durable, params={})
      raise JetStream::Error::InvalidDurableName.new("nats: invalid durable name") if durable.empty?
      params[:consumer] ||= durable
      stream = params[:stream].nil? ? find_stream_name_by_subject(subject) : params[:stream]

      begin
        consumer_info(stream, params[:consumer])
      rescue NATS::JetStream::Error::NotFound => e
        # If attempting to bind, then this is a hard error.
        raise e if params[:stream]

        config = if not params[:config]
                   JetStream::API::ConsumerConfig.new
                 elsif params[:config].is_a?(JetStream::API::ConsumerConfig)
                   params[:config]
                 else
                   JetStream::API::ConsumerConfig.new(params[:config])
                 end
        config[:durable_name] = durable
        config[:ack_policy] ||= JS::Config::AckExplicit
        add_consumer(stream, config)
      end

      deliver = @nc.new_inbox
      sub = @nc.subscribe(deliver)
      sub.extend(PullSubscription)

      consumer = params[:consumer]
      subject = "#{@prefix}.CONSUMER.MSG.NEXT.#{stream}.#{consumer}"
      sub.jsi = JS::Sub.new(
        js: self,
        stream: stream,
        consumer: params[:consumer],
        nms: subject
      )
      sub
    end

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
        result = api_request(req_subject, config.to_json, params)
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
        req_subject = if config[:durable_name]
                        "#{@prefix}.CONSUMER.DURABLE.CREATE.#{stream}.#{config[:durable_name]}"
                      else
                        "#{@prefix}.CONSUMER.CREATE.#{stream}"
                      end

        config[:ack_policy] ||= JS::Config::AckExplicit
        # Check if have to normalize ack wait so that it is in nanoseconds for Go compat.
        if config[:ack_wait]
          raise ArgumentError.new("nats: invalid ack wait") unless config[:ack_wait].is_a?(Integer)
          config[:ack_wait] = config[:ack_wait] * 1_000_000_000
        end

        req = {
          stream_name: stream,
          config: config
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

      def get_last_msg(stream_name, subject)
        req_subject = "#{@prefix}.STREAM.MSG.GET.#{stream_name}"
        req = {'last_by_subj': subject}
        data = req.to_json
        resp = api_request(req_subject, data)
        JetStream::API::RawStreamMsg.new(resp[:message])
      end

      private

      def api_request(req_subject, req="", params={})
        params[:timeout] ||= @opts[:timeout]
        result = begin
                   msg = @nc.request(req_subject, req, **params)
                   JSON.parse(msg.data, symbolize_names: true)
                 rescue NATS::IO::NoRespondersError
                   raise JetStream::Error::ServiceUnavailable
                 end
        raise JS.from_error(result[:error]) if result[:error]

        result
      end
    end

    # PushSubscription is included into NATS::Subscription so that it
    #
    # @example Create a push subscription using JetStream context.
    #
    #   require 'nats/client'
    #
    #   nc = NATS.connect
    #   js = nc.jetstream
    #   sub = js.subscribe("foo", "bar")
    #   msg = sub.next_msg
    #   msg.ack
    #   sub.unsubscribe
    #
    # @!visibility public
    module PushSubscription
      # consumer_info retrieves the current status of the pull subscription consumer.
      # @param params [Hash] Options to customize API request.
      # @option params [Float] :timeout Time to wait for response.
      # @return [JetStream::API::ConsumerInfo] The latest ConsumerInfo of the consumer.
      def consumer_info(params={})
        @jsi.js.consumer_info(@jsi.stream, @jsi.consumer, params)
      end
    end
    private_constant :PushSubscription

    # PullSubscription is included into NATS::Subscription so that it
    # can be used to fetch messages from a pull based consumer from
    # JetStream.
    #
    # @example Create a pull subscription using JetStream context.
    #
    #   require 'nats/client'
    #
    #   nc = NATS.connect
    #   js = nc.jetstream
    #   psub = js.pull_subscribe("foo", "bar")
    #
    #   loop do
    #     msgs = psub.fetch(5)
    #     msgs.each do |msg|
    #       msg.ack
    #     end
    #   end
    #
    # @!visibility public
    module PullSubscription
      # next_msg is not available for pull based subscriptions.
      # @raise [NATS::JetStream::Error]
      def next_msg(params={})
        raise ::NATS::JetStream::Error.new("nats: pull subscription cannot use next_msg")
      end

      # fetch makes a request to be delivered more messages from a pull consumer.
      #
      # @param batch [Fixnum] Number of messages to pull from the stream.
      # @param params [Hash] Options to customize the fetch request.
      # @option params [Float] :timeout Duration of the fetch request before it expires.
      # @return [Array<NATS::Msg>]
      def fetch(batch=1, params={})
        if batch < 1
          raise ::NATS::JetStream::Error.new("nats: invalid batch size")
        end

        t = MonotonicTime.now
        timeout = params[:timeout] ||= 5
        expires = (timeout * 1_000_000_000) - 100_000
        next_req = {
          batch: batch
        }

        msgs = []
        case
        when batch < 1
          raise ::NATS::JetStream::Error.new("nats: invalid batch size")
        when batch == 1
          ####################################################
          # Fetch (1)                                        #
          ####################################################

          # Check if there is any pending message in the queue that is
          # ready to be consumed.
          synchronize do
            unless @pending_queue.empty?
              msg = @pending_queue.pop
              # Check for a no msgs response status.
              if JS.is_status_msg(msg)
                case msg.header["Status"]
                when JS::Status::NoMsgs
                  msg = nil
                when JS::Status::RequestTimeout
                  # Skip
                else
                  raise JS.from_msg(msg)
                end
              else
                msgs << msg
              end
            end
          end

          # Make lingering request with expiration.
          next_req[:expires] = expires
          if msgs.empty?
            # Make publish request and wait for response.
            @nc.publish(@jsi.nms, JS.next_req_to_json(next_req), @subject)

            # Wait for result of fetch or timeout.
            synchronize { wait_for_msgs_cond.wait(timeout) }

            msgs << @pending_queue.pop unless @pending_queue.empty?

            duration = MonotonicTime.since(t)
            if duration > timeout
              raise ::NATS::Timeout.new("nats: fetch timeout")
            end

            # Should have received at least a message at this point,
            # if that is not the case then error already.
            if JS.is_status_msg(msgs.first)
              msg = msgs.first
              case msg.header[JS::Header::Status]
              when JS::Status::RequestTimeout
                raise NATS::Timeout.new("nats: fetch request timeout")
              else
                raise JS.from_msg(msgs.first)
              end
            end
          end
        when batch > 1
          ####################################################
          # Fetch (n)                                        #
          ####################################################

          # Check if there already enough in the pending buffer.
          synchronize do
            if batch <= @pending_queue.size
              batch.times do
                msg = @pending_queue.pop

                # Check for a no msgs response status.
                if JS.is_status_msg(msg)
                  case msg.header[JS::Header::Status]
                  when JS::Status::NoMsgs, JS::Status::RequestTimeout
                    # Skip these
                    next
                  else
                    raise JS.from_msg(msg)
                  end
                else
                  msgs << msg
                end
              end

              return msgs
            end
          end

          # Make publish request and wait any response.
          next_req[:no_wait] = true
          @nc.publish(@jsi.nms, JS.next_req_to_json(next_req), @subject)

          # Not receiving even one is a timeout.
          start_time = MonotonicTime.now
          msg = nil
          synchronize {
            wait_for_msgs_cond.wait(timeout)
            msg = @pending_queue.pop unless @pending_queue.empty?
          }

          # Check if the first message was a response saying that
          # there are no messages.
          if !msg.nil? && JS.is_status_msg(msg)
            case msg.header[JS::Header::Status]
            when JS::Status::NoMsgs
              # Make another request that does wait.
              next_req[:expires] = expires
              next_req.delete(:no_wait)

              @nc.publish(@jsi.nms, JS.next_req_to_json(next_req), @subject)
            when JS::Status::RequestTimeout
              raise NATS::Timeout.new("nats: fetch request timeout")
            else
              raise JS.from_msg(msg)
            end
          else
            msgs << msg unless msg.nil?
          end

          # Check if have not received yet a single message.
          duration = MonotonicTime.since(start_time)
          if msgs.empty? and duration > timeout
            raise NATS::Timeout.new("nats: fetch timeout")
          end

          needed = batch - msgs.count
          while needed > 0 and MonotonicTime.since(start_time) < timeout
            duration = MonotonicTime.since(start_time)

            # Wait for the rest of the messages.
            synchronize do

              # Wait until there is a message delivered.
              if @pending_queue.empty?
                deadline = timeout - duration
                wait_for_msgs_cond.wait(deadline) if deadline > 0

                duration = MonotonicTime.since(start_time)
                if msgs.empty? && @pending_queue.empty? and duration > timeout
                  raise NATS::Timeout.new("nats: fetch timeout")
                end
              else
                msg = @pending_queue.pop

                if JS.is_status_msg(msg)
                  case msg.header[JS::Header::Status]
                  when JS::Status::NoMsgs, JS::Status::RequestTimeout
                    duration = MonotonicTime.since(start_time)

                    if duration > timeout
                      # Only received a subset of the messages.
                      if !msgs.empty?
                        return msgs
                      else
                        raise NATS::Timeout.new("nats: fetch timeout")
                      end
                    end
                  else
                    raise JS.from_msg(msg)
                  end

                else
                  # Add to the set of messages that will be returned.
                  msgs << msg
                  needed -= 1
                end
              end
            end # :end: synchronize
          end
        end

        msgs
      end

      # consumer_info retrieves the current status of the pull subscription consumer.
      # @param params [Hash] Options to customize API request.
      # @option params [Float] :timeout Time to wait for response.
      # @return [JetStream::API::ConsumerInfo] The latest ConsumerInfo of the consumer.
      def consumer_info(params={})
        @jsi.js.consumer_info(@jsi.stream, @jsi.consumer, params)
      end
    end
    private_constant :PullSubscription

    #######################################
    #                                     #
    #  JetStream Message and Ack Methods  #
    #                                     #
    #######################################

    # JetStream::Msg module includes the methods so that a regular NATS::Msg
    # can be enhanced with JetStream features like acking and metadata.
    module Msg
      module Ack
        # Ack types
        Ack      = ("+ACK".freeze)
        Nak      = ("-NAK".freeze)
        Progress = ("+WPI".freeze)
        Term     = ("+TERM".freeze)

        Empty = (''.freeze)
        DotSep = ('.'.freeze)
        NoDomainName = ('_'.freeze)

        # Position
        Prefix0 = ('$JS'.freeze)
        Prefix1 = ('ACK'.freeze)
        Domain = 2
        AccHash = 3
        Stream = 4
        Consumer = 5
        NumDelivered = 6
        StreamSeq = 7
        ConsumerSeq = 8
        Timestamp = 9
        NumPending = 10

        # Subject without domain:
        # $JS.ACK.<stream>.<consumer>.<delivered>.<sseq>.<cseq>.<tm>.<pending>
        #
        V1TokenCounts = 9

        # Subject with domain:
        # $JS.ACK.<domain>.<account hash>.<stream>.<consumer>.<delivered>.<sseq>.<cseq>.<tm>.<pending>.<a token with a random value>
        #
        V2TokenCounts = 12

        SequencePair = Struct.new(:stream, :consumer)
      end
      private_constant :Ack

      class Metadata
        attr_reader :sequence, :num_delivered, :num_pending, :timestamp, :stream, :consumer, :domain

        def initialize(opts)
          @sequence      = Ack::SequencePair.new(opts[Ack::StreamSeq].to_i, opts[Ack::ConsumerSeq].to_i)
          @domain        = opts[Ack::Domain]
          @num_delivered = opts[Ack::NumDelivered].to_i
          @num_pending   = opts[Ack::NumPending].to_i
          @timestamp     = Time.at((opts[Ack::Timestamp].to_i / 1_000_000_000.0))
          @stream        = opts[Ack::Stream]
          @consumer      = opts[Ack::Consumer]
          # TODO: Not exposed in Go client either right now.
          # account      = opts[Ack::AccHash]
        end
      end

      module AckMethods
        def ack(**params)
          ensure_is_acked_once!

          resp = if params[:timeout]
                   @nc.request(@reply, Ack::Ack, **params)
                 else
                   @nc.publish(@reply, Ack::Ack)
                 end
          @sub.synchronize { @ackd = true }

          resp
        end

        def ack_sync(**params)
          ensure_is_acked_once!

          params[:timeout] ||= 0.5
          resp = @nc.request(@reply, Ack::Ack, **params)
          @sub.synchronize { @ackd = true }

          resp
        end

        def nak(**params)
          ensure_is_acked_once!

          resp = if params[:timeout]
                   @nc.request(@reply, Ack::Nak, **params)
                 else
                   @nc.publish(@reply, Ack::Nak)
                 end
          @sub.synchronize { @ackd = true }

          resp
        end

        def term(**params)
          ensure_is_acked_once!

          resp = if params[:timeout]
                   @nc.request(@reply, Ack::Term, **params)
                 else
                   @nc.publish(@reply, Ack::Term)
                 end
          @sub.synchronize { @ackd = true }

          resp
        end

        def in_progress(**params)
          params[:timeout] ? @nc.request(@reply, Ack::Progress, **params) : @nc.publish(@reply, Ack::Progress)
        end

        def metadata
          @meta ||= parse_metadata(reply)
        end

        private

        def ensure_is_acked_once!
          @sub.synchronize do
            if @ackd
              raise JetStream::Error::MsgAlreadyAckd.new("nats: message was already acknowledged: #{self}")
            end
          end
        end

        def parse_metadata(reply)
          tokens = reply.split(Ack::DotSep)
          n = tokens.count

          case
          when n < Ack::V1TokenCounts || (n > Ack::V1TokenCounts and n < Ack::V2TokenCounts)
            raise NotJSMessage.new("nats: not a jetstream message")
          when tokens[0] != Ack::Prefix0 || tokens[1] != Ack::Prefix1
            raise NotJSMessage.new("nats: not a jetstream message")
          when n == Ack::V1TokenCounts
            tokens.insert(Ack::Domain, Ack::Empty)
            tokens.insert(Ack::AccHash, Ack::Empty)
          when tokens[Ack::Domain] == Ack::NoDomainName
            tokens[Ack::Domain] = Ack::Empty
          end

          Metadata.new(tokens)
        end
      end
    end

    ####################################
    #                                  #
    # JetStream Configuration Options  #
    #                                  #
    ####################################

    # Misc internal functions to support JS API.
    # @private
    module JS
      DefaultAPIPrefix = ("$JS.API".freeze)

      module Status
        CtrlMsg        = ("100".freeze)
        NoMsgs         = ("404".freeze)
        NotFound       = ("404".freeze)
        RequestTimeout = ("408".freeze)
        ServiceUnavailable = ("503".freeze)
      end

      module Header
        Status              = ("Status".freeze)
        Desc                = ("Description".freeze)
        MsgID               = ("Nats-Msg-Id".freeze)
        ExpectedStream      = ("Nats-Expected-Stream".freeze)
        ExpectedLastSeq     = ("Nats-Expected-Last-Sequence".freeze)
        ExpectedLastSubjSeq = ("Nats-Expected-Last-Subject-Sequence".freeze)
        ExpectedLastMsgID   = ("Nats-Expected-Last-Msg-Id".freeze)
        LastConsumerSeq     = ("Nats-Last-Consumer".freeze)
        LastStreamSeq       = ("Nats-Last-Stream".freeze)
      end

      module Config
        # AckPolicy
        AckExplicit = ("explicit".freeze)
        AckAll      = ("all".freeze)
        AckNone     = ("none".freeze)
      end

      class Sub
        attr_reader :js, :stream, :consumer, :nms

        def initialize(opts={})
          @js = opts[:js]
          @stream = opts[:stream]
          @consumer = opts[:consumer]
          @nms = opts[:nms]
        end
      end

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

    #####################
    #                   #
    # JetStream Errors  #
    #                   #
    #####################

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

    #######################
    #                     #
    # JetStream API Types #
    #                     #
    #######################

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
          opts[:config][:ack_wait] = opts[:config][:ack_wait] / 1_000_000_000
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
      ConsumerConfig = Struct.new(:durable_name, :description, :deliver_subject,
                                  :deliver_group, :deliver_policy, :opt_start_seq,
                                  :opt_start_time, :ack_policy, :ack_wait, :max_deliver,
                                  :filter_subject, :replay_policy, :rate_limit_bps,
                                  :sample_freq, :max_waiting, :max_ack_pending,
                                  :flow_control, :idle_heartbeat, :headers_only,
                                  keyword_init: true) do
        def initialize(opts={})
          # Filter unrecognized fields just in case.
          rem = opts.keys - members
          opts.delete_if { |k| rem.include?(k) }
          super(opts)
        end

        def to_json(*args)
          config = self.to_h
          config.delete_if { |_k, v| v.nil? }
          config.to_json(*args)
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
      StreamConfig = Struct.new(:name, :subjects, :retention, :max_consumers,
                                :max_msgs, :max_bytes, :max_age,
                                :max_msgs_per_subject, :max_msg_size,
                                :discard, :storage, :num_replicas, :duplicate_window,
                                keyword_init: true) do
        def initialize(opts={})
          # Filter unrecognized fields just in case.
          rem = opts.keys - members
          opts.delete_if { |k| rem.include?(k) }
          super(opts)
        end

        def to_json(*args)
          config = self.to_h
          config.delete_if { |_k, v| v.nil? }
          config.to_json(*args)
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
          opts[:created] = Time.parse(opts[:created])

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
