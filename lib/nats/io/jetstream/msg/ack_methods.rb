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
    module Msg
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
          payload = if params[:delay]
                      payload = "#{Ack::Nak} #{{ delay: params[:delay] }.to_json}"
                    else
                      Ack::Nak
                    end
          resp = if params[:timeout]
                   @nc.request(@reply, payload, **params)
                 else
                   @nc.publish(@reply, payload)
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
  end
end
