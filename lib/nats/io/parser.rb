# Copyright 2016-2018 The NATS Authors
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
  module Protocol

    MSG      = /\AMSG\s+([^\s]+)\s+([^\s]+)\s+(([^\s]+)[^\S\r\n]+)?(\d+)\r\n/i
    HMSG     = /\AHMSG\s+([^\s]+)\s+([^\s]+)\s+(([^\s]+)[^\S\r\n]+)?([\d]+)\s+(\d+)\r\n/i
    OK       = /\A\+OK\s*\r\n/i
    ERR      = /\A-ERR\s+('.+')?\r\n/i
    PING     = /\APING\s*\r\n/i
    PONG     = /\APONG\s*\r\n/i
    INFO     = /\AINFO\s+([^\r\n]+)\r\n/i
    UNKNOWN  = /\A(.*)\r\n/

    AWAITING_CONTROL_LINE = 1
    AWAITING_MSG_PAYLOAD  = 2

    CR_LF = ("\r\n".freeze)
    CR_LF_SIZE = (CR_LF.bytesize)

    PING_REQUEST  = ("PING#{CR_LF}".freeze)
    PONG_RESPONSE = ("PONG#{CR_LF}".freeze)

    SUB_OP = ('SUB'.freeze)
    EMPTY_MSG = (''.freeze)

    class Parser
      def initialize(nc)
        @nc = nc
        reset!
      end

      def reset!
        @buf = nil
        @parse_state = AWAITING_CONTROL_LINE

        @sub = nil
        @sid = nil
        @reply = nil
        @needed = nil
        @header_needed = nil
      end

      def parse(data)
        @buf = @buf ? @buf << data : data
        while (@buf)
          case @parse_state
          when AWAITING_CONTROL_LINE
            case @buf
            when MSG
              @buf = $'
              @sub, @sid, @reply, @needed = $1, $2.to_i, $4, $5.to_i
              @parse_state = AWAITING_MSG_PAYLOAD
            when HMSG
              @buf = $'
              @sub, @sid, @reply, @header_needed, @needed = $1, $2.to_i, $4, $5.to_i, $6.to_i
              @parse_state = AWAITING_MSG_PAYLOAD
            when OK # No-op right now
              @buf = $'
            when ERR
              @buf = $'
              @nc.process_err($1)
            when PING
              @buf = $'
              @nc.process_ping
            when PONG
              @buf = $'
              @nc.process_pong
            when INFO
              @buf = $'
              # First INFO message is processed synchronously on connect,
              # and onwards we would be receiving asynchronously INFO commands
              # signaling possible changes in the topology of the NATS cluster.
              @nc.process_info($1)
            when UNKNOWN
              @buf = $'
              @nc.process_err("Unknown protocol: #{$1}")
            else
              # If we are here we do not have a complete line yet that we understand.
              return
            end
            @buf = nil if (@buf && @buf.empty?)

          when AWAITING_MSG_PAYLOAD
            return unless (@needed && @buf.bytesize >= (@needed + CR_LF_SIZE))
            if @header_needed
              hbuf = @buf.slice(0, @header_needed)
              payload = @buf.slice(@header_needed, (@needed-@header_needed))
              @nc.process_msg(@sub, @sid, @reply, payload, hbuf)
              @buf = @buf.slice((@needed + CR_LF_SIZE), @buf.bytesize)
            else
              @nc.process_msg(@sub, @sid, @reply, @buf.slice(0, @needed), nil)
              @buf = @buf.slice((@needed + CR_LF_SIZE), @buf.bytesize)
            end

            @sub = @sid = @reply = @needed = @header_needed = nil
            @parse_state = AWAITING_CONTROL_LINE
            @buf = nil if (@buf && @buf.empty?)
          end
        end
      end
    end
  end
end
