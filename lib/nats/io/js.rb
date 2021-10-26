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
require 'time'

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
    def publish(subject, payload="", params={})
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
        resp = @nc.request_msg(msg, params)
        result = JSON.parse(resp.data, symbolize_names: true)
      rescue ::NATS::IO::NoRespondersError
        raise JetStream::Error::NoStreamResponse.new("nats: no response from stream")
      end
      raise JS.from_error(result[:error]) if result[:error]

      PubAck.new(result)
    end

    # pull_subscribe binds or creates a subscription to a JetStream pull consumer.
    #
    # @param subject [String] Subject from which the messages will be fetched.
    # @param durable [String] Consumer durable name from where the messages will be fetched.
    # @param params [Hash] Options to customize the PullSubscription.
    # @option params [String] :stream Name of the Stream to which the consumer belongs.
    # @option params [String] :consumer Name of the Consumer to which the PullSubscription will be bound.
    # @return [NATS::JetStream::PullSubscription]
    def pull_subscribe(subject, durable, params={})
      raise JetStream::Error::InvalidDurableName.new("nats: invalid durable name") if durable.empty?
      params[:consumer] ||= durable
      params[:stream] ||= find_stream_name_by_subject(subject)

      # Assert that the consumer is present.
      consumer_info(params[:stream], params[:consumer])

      deliver = @nc.new_inbox
      sub = @nc.subscribe(deliver)
      sub.extend(PullSubscription)

      stream = params[:stream]
      consumer = params[:consumer]
      subject = "#{@prefix}.CONSUMER.MSG.NEXT.#{stream}.#{consumer}"
      sub.jsi = JS::Sub.new(
        js: self,
        stream: params[:stream],
        consumer: params[:consumer],
        nms: subject
      )
      sub
    end

    # A JetStream::Manager can be used to make requests to the JetStream API.
    module Manager

      # consumer_info retrieves the current status of a consumer.
      # @return [JetStream::API::ConsumerInfo] The latest ConsumerInfo of the consumer.
      def consumer_info(stream, consumer, params={})
        raise JetStream::Error::InvalidStreamName.new("nats: invalid stream name") if stream.nil? or stream.empty?
        raise JetStream::Error::InvalidConsumerName.new("nats: invalid consumer name") if consumer.nil? or consumer.empty?

        subject = "#{@prefix}.CONSUMER.INFO.#{stream}.#{consumer}"
        params[:timeout] ||= @opts[:timeout]

        begin
          msg = @nc.request(subject, '', timeout: params[:timeout])
          result = JSON.parse(msg.data, symbolize_names: true)
        rescue NATS::IO::NoRespondersError
          raise JetStream::Error::ServiceUnavailable
        end
        raise JS.from_error(result[:error]) if result[:error]

        JetStream::API::ConsumerInfo.new(result)
      end

      # find_stream_name_by_subject does a lookup for the stream to which
      # the subject belongs.
      # @param [String] The subject that belongs to a stream.
      # @param [Hash] Options to customize API request.
      # @option [Float] :timeout Time to wait for response.
      # @return [String] The name of the JetStream stream for the subject.
      def find_stream_name_by_subject(subject, params={})
        req_subject = "#{@prefix}.STREAM.NAMES"
        req = { subject: subject }

        params[:timeout] ||= @opts[:timeout]
        begin
          msg = @nc.request(req_subject, req.to_json, params)
          result = JSON.parse(msg.data, symbolize_names: true)
        rescue NATS::IO::NoRespondersError
          raise JetStream::Error::ServiceUnavailable
        end
        raise JS.from_error(result[:error]) if result[:error]
        raise JetStream::Error::NotFound unless result[:streams]

        result[:streams].first
      end
    end

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
              raise JS.from_msg(msgs.first)
            end
          end
        when batch > 1
          ####################################################
          # Fetch (n)                                        #
          ####################################################

          # Check if there already enough in the pending buffer.
          synchronize do
            if batch <= @pending_queue.size
              batch.times { msgs << @pending_queue.pop }

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
            else
              raise JS.from_msg(msg)
            end
          else
            msgs << msg
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
                  case msg.header["Status"]
                  when JS::Status::NoMsgs, JS::Status::RequestTimeout
                    duration = MonotonicTime.since(start_time)

                    # Do not time out if we received at least some messages.
                    if msgs.empty? && @pending_queue.empty? and duration > timeout
                      raise NATS::Timeout.new("nats: fetch timeout")
                    end

                    # Likely only received a subset of the messages.
                    return msgs
                  else
                    raise JS.from_msg(msg)
                  end
                else
                  msgs << msg
                  needed -= 1
                end
              end
            end # :end: synchronize
          end
        end

        msgs
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
        def ack(params={})
          ensure_is_acked_once!

          resp = if params[:timeout]
                   @nc.request(@reply, Ack::Ack, params)
                 else
                   @nc.publish(@reply, Ack::Ack)
                 end
          @sub.synchronize { @ackd = true }

          resp
        end

        def ack_sync(params={})
          ensure_is_acked_once!

          params[:timeout] ||= 0.5
          resp = @nc.request(@reply, Ack::Ack, params)
          @sub.synchronize { @ackd = true }

          resp
        end

        def nak(params={})
          ensure_is_acked_once!

          resp = if params[:timeout]
                   @nc.request(@reply, Ack::Nak, params)
                 else
                   @nc.publish(@reply, Ack::Nak)
                 end
          @sub.synchronize { @ackd = true }

          resp
        end

        def term(params={})
          ensure_is_acked_once!

          resp = if params[:timeout]
                   @nc.request(@reply, Ack::Term, params)
                 else
                   @nc.publish(@reply, Ack::Term)
                 end
          @sub.synchronize { @ackd = true }

          resp
        end

        def in_progress(params={})
          params[:timeout] ? @nc.request(@reply, Ack::Progress, params) : @nc.publish(@reply, Ack::Progress)
        end

        def metadata
          @meta ||= parse_metadata(reply)
        end

        private

        def ensure_is_acked_once!
          @sub.synchronize { raise JetStream::Error::InvalidJSAck.new("nats: invalid ack") if @ackd }
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

      # When an ack is no longer valid, for example when it has been acked already.
      class InvalidJSAck < Error; end

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
      SequenceInfo = Struct.new(:consumer_seq, :stream_seq, :last_active, keyword_init: true) do
        def initialize(opts={})
          # Filter unrecognized fields just in case.
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
                                :num_ack_pending, :num_redelivered, :num_waiting, :num_pending,
                                :cluster, :push_bound, keyword_init: true) do
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
      ConsumerConfig = Struct.new(:durable_name, :deliver_policy,
                                  :ack_policy, :ack_wait, :max_deliver,
                                  :replay_policy, :max_waiting,
                                  :max_ack_pending, keyword_init: true) do 
        def initialize(opts={})
          # Filter unrecognized fields just in case.
          rem = opts.keys - members
          opts.delete_if { |k| rem.include?(k) }
          super(opts)
        end
      end
    end
  end
end
