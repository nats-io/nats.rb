module NATS
  module Protocol

    MSG      = /\AMSG\s+([^\s]+)\s+([^\s]+)\s+(([^\s]+)[^\S\r\n]+)?(\d+)\r\n/i
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
            @nc.process_msg(@sub, @sid, @reply, @buf.slice(0, @needed))
            @buf = @buf.slice((@needed + CR_LF_SIZE), @buf.bytesize)
            @sub = @sid = @reply = @needed = nil
            @parse_state = AWAITING_CONTROL_LINE
            @buf = nil if (@buf && @buf.empty?)
          end
        end
      end
    end
  end
end
