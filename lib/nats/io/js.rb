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
    attr_reader :opts, :prefix

    def initialize(conn, params={})
      @nc = conn
      @prefix = params[:prefix] ||= JS::DefaultAPIPrefix
      @opts = params
      @jsm = JSM.new(conn, params)
    end

    # publish produces a message for JetStream.
    def publish(subject, payload=EMPTY_MSG, params={})
      params[:timeout] ||= 0.5
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
        raise JetStream::NoStreamResponseError.new("nats: no response from stream")
      end
      raise JetStream::APIError.new(result[:error]) if result[:error]

      result
    end

    def pull_subscribe(subject, durable, params={})
      raise JetStream::InvalidDurableNameError if durable.empty?
      params[:consumer] ||= durable

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

    class JSM
      def initialize(conn=nil, params={})
        @prefix = params[:prefix]
        @nc = conn
      end
    end
    private_constant :JSM

    module PullSubscription
      def next_msg(params={})
        raise JetStream::Error.new("nats: pull subscription cannot use next_msg")
      end

      def fetch(batch, params={})
        if batch < 1
          raise JetStream::Error.new("nats: invalid batch size")
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
          raise JetStream::Error.new("nats: invalid batch size")
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
                else raise APIError.from_msg(msg)
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
              raise NATS::IO::Timeout.new("nats: fetch timeout")
            end

            # Should have received at least a message at this point,
            # if that is not the case then error already.
            if JS.is_status_msg(msgs.first)
              raise APIError.from_msg(msgs.first)
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
            else raise APIError.from_msg(msg)
            end
          else
            msgs << msg
          end

          # Check if have not received yet a single message.
          duration = MonotonicTime.since(start_time)
          if msgs.empty? and duration > timeout
            raise NATS::IO::Timeout.new("nats: fetch timeout")
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
                  raise NATS::IO::Timeout.new("nats: fetch timeout")
                end
             else
                msg = @pending_queue.pop

                if JS.is_status_msg(msg)
                  case msg.header["Status"]
                  when JS::Status::NoMsgs, JS::Status::RequestTimeout
                    duration = MonotonicTime.since(start_time)

                    # Do not time out if we received at least some messages.
                    if msgs.empty? && @pending_queue.empty? and duration > timeout
                      raise NATS::IO::Timeout.new("nats: fetch timeout")
                    end

                    # Likely only received a subset of the messages.
                    return msgs
                  else raise APIError.from_msg(msg)
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

    #####################
    #                   #
    # JetStream Errors  #
    #                   #
    #####################

    class Error < StandardError; end

    # When there is a no responders error on a publish.
    class NoStreamResponseError < Error; end

    class InvalidDurableNameError < Error; end

    class InvalidJSAck < Error; end

    class NotJSMessage < Error; end

    class APIError < Error
      attr_reader :code, :err_code, :description, :stream, :seq

      class << self
        # header={"Status"=>"408", "Description"=>"Request Timeout"})>
        def from_msg(msg)
          return unless msg and msg.header
          return APIError.new({
              code: msg.header["Status"],
              description: msg.header["Description"]
            })
        end
      end

      def initialize(params={})
        @code = params[:code]
        @err_code = params[:err_code]
        @description = params[:description]
        @stream = params[:stream]
        @seq = params[:seq]
      end

      def to_s
        "nats: #{@description}"
      end

      def inspect
        "#<NATS::JetStream::APIError(code: #{@code}, err_code: #{@err_code}, description: '#{@description}')>"
      end
    end

    ####################################
    #                                  #
    # JetStream Configuration Options  #
    #                                  #
    ####################################
    module JS
      DefaultAPIPrefix = ("$JS.API".freeze)

      module Status
        CtrlMsg        = ("100".freeze)
        NoMsgs         = ("404".freeze)
        RequestTimeout = ("408".freeze)
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

      class << self
        def next_req_to_json(next_req)
          req = {}
          req['batch'] = next_req[:batch]
          req['expires'] = next_req[:expires].to_i if next_req[:expires]
          req['no_wait'] = next_req[:no_wait] if next_req[:no_wait]
          req.to_json
        end

        def is_status_msg(msg)
          return (!msg.nil? and (!msg.header.nil? and msg.header["Status"]))
        end
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
    end
    private_constant :JS

    class MsgMetadata
      attr_reader :sequence, :num_delivered, :num_pending, :timestamp, :stream, :consumer, :domain

      module Ack
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
        # Subject with domain:
        # $JS.ACK.<domain>.<account hash>.<stream>.<consumer>.<delivered>.<sseq>.<cseq>.<tm>.<pending>.<a token with a random value>
        #
        V1TokenCounts = 9
        V2TokenCounts = 12

        class << self
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

            MsgMetadata.new(tokens)
          end
        end
      end
      private_constant :Ack

      SequencePair = Struct.new(:stream, :consumer)

      def initialize(opts)
        @sequence      = SequencePair.new(opts[Ack::StreamSeq].to_i, opts[Ack::ConsumerSeq].to_i)
        @domain        = opts[Ack::Domain]
        @num_delivered = opts[Ack::NumDelivered].to_i
        @num_pending   = opts[Ack::NumPending].to_i
        @timestamp     = Time.at((opts[Ack::Timestamp].to_i / 1_000_000_000.0))
        @stream        = opts[Ack::Stream]
        @consumer      = opts[Ack::Consumer]
        # TODO: Not exposed in Go client either right now.
        # @account       = opts[Ack::AccHash]
      end

      class << self
        def parse_metadata(reply)
          Ack.parse_metadata(reply)
        end
      end
    end
  end
end

