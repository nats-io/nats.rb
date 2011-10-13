module NATSD #:nodoc: all

  module Connection #:nodoc: all

    attr_accessor :in_msgs, :out_msgs, :in_bytes, :out_bytes
    attr_reader :cid, :closing, :last_activity

    alias :closing? :closing

    def client_info
      @client_info ||= Socket.unpack_sockaddr_in(get_peername)
    end

    def info
      {
        :cid => cid,
        :ip => client_info[1],
        :port => client_info[0],
        :subscriptions => @subscriptions.size,
        :pending_size => get_outbound_data_size,
        :in_msgs => in_msgs,
        :out_msgs => out_msgs,
        :in_bytes => in_bytes,
        :out_bytes => out_bytes
      }
    end

    def post_init
      @cid = Server.cid
      @subscriptions = {}
      @verbose = @pedantic = true # suppressed by most clients, but allows friendly telnet
      @in_msgs = @out_msgs = @in_bytes = @out_bytes = 0
      @parse_state = AWAITING_CONTROL_LINE
      send_info
      @auth_pending = EM.add_timer(NATSD::Server.auth_timeout) { connect_auth_timeout } if Server.auth_required?
      @ping_timer = EM.add_periodic_timer(NATSD::Server.ping_interval) { send_ping }
      @pings_outstanding = 0
      Server.num_connections += 1
      debug "Client connection created", client_info, cid
    end

    def send_ping
      return if @closing
      if @pings_outstanding > NATSD::Server.ping_max
        error_close UNRESPONSIVE
        return
      end
      send_data(PING_RESPONSE)
      @pings_outstanding += 1
    end

    def connect_auth_timeout
      error_close AUTH_REQUIRED
      debug "Client connection timeout due to lack of auth credentials", cid
    end

    def receive_data(data)
      @buf = @buf ? @buf << data : data
      return close_connection if @buf =~ /(\006|\004)/ # ctrl+c or ctrl+d for telnet friendly

      while (@buf && !@closing)
        case @parse_state
        when AWAITING_CONTROL_LINE
          case @buf
          when PUB_OP
            ctrace('PUB OP', strip_op($&)) if NATSD::Server.trace_flag?
            return connect_auth_timeout if @auth_pending
            @buf = $'
            @parse_state = AWAITING_MSG_PAYLOAD
            @msg_sub, @msg_reply, @msg_size = $1, $3, $4.to_i
            if (@msg_size > NATSD::Server.max_payload)
              debug "Message payload size exceeded (#{@msg_size}/#{NATSD::Server.max_payload}), closing client connection..", cid
              error_close PAYLOAD_TOO_BIG
            end
            send_data(INVALID_SUBJECT) if (@pedantic && !(@msg_sub =~ SUB_NO_WC))
          when SUB_OP
            ctrace('SUB OP', strip_op($&)) if NATSD::Server.trace_flag?
            return connect_auth_timeout if @auth_pending
            @buf = $'
            sub, qgroup, sid = $1, $3, $4
            return send_data(INVALID_SUBJECT) if !($1 =~ SUB)
            return send_data(INVALID_SID_TAKEN) if @subscriptions[sid]
            sub = Subscriber.new(self, sub, sid, qgroup, 0)
            @subscriptions[sid] = sub
            Server.subscribe(sub)
            send_data(OK) if @verbose
          when UNSUB_OP
            ctrace('UNSUB OP', strip_op($&)) if NATSD::Server.trace_flag?
            return connect_auth_timeout if @auth_pending
            @buf = $'
            sid, sub = $1, @subscriptions[$1]
            if sub
              # If we have set max_responses, we will unsubscribe once we have received
              # the appropriate amount of responses.
              sub.max_responses = ($2 && $3) ? $3.to_i : nil
              delete_subscriber(sub) unless (sub.max_responses && (sub.num_responses < sub.max_responses))
              send_data(OK) if @verbose
            else
              send_data(INVALID_SID_NOEXIST) if @pedantic
            end
          when PING
            ctrace('PING OP', strip_op($&)) if NATSD::Server.trace_flag?
            @buf = $'
            send_data(PONG_RESPONSE)
          when PONG
            ctrace('PONG OP', strip_op($&)) if NATSD::Server.trace_flag?
            @buf = $'
            @pings_outstanding -= 1
          when CONNECT
            ctrace('CONNECT OP', strip_op($&)) if NATSD::Server.trace_flag?
            @buf = $'
            begin
              config = JSON.parse($1)
              process_connect_config(config)
            rescue => e
              send_data(INVALID_CONFIG)
              log_error
            end
          when INFO
            ctrace('INFO OP', strip_op($&)) if NATSD::Server.trace_flag?
            return connect_auth_timeout if @auth_pending
            @buf = $'
            send_info
          when UNKNOWN
            ctrace('Unknown Op', strip_op($&)) if NATSD::Server.trace_flag?
            return connect_auth_timeout if @auth_pending
            @buf = $'
            send_data(UNKNOWN_OP)
          else
            # If we are here we do not have a complete line yet that we understand.
            # If too big, cut the connection off.
            if @buf.bytesize > NATSD::Server.max_control_line
              debug "Control line size exceeded (#{@buf.bytesize}/#{NATSD::Server.max_control_line}), closing client connection..", cid
              error_close PROTOCOL_OP_TOO_BIG
            end
            return
          end
          @buf = nil if (@buf && @buf.empty?)

        when AWAITING_MSG_PAYLOAD
          return unless (@buf.bytesize >= (@msg_size + CR_LF_SIZE))
          msg = @buf.slice(0, @msg_size)
          ctrace('Processing msg', @msg_sub, @msg_reply, msg) if NATSD::Server.trace_flag?
          send_data(OK) if @verbose
          Server.route_to_subscribers(@msg_sub, @msg_reply, msg)
          @in_msgs += 1
          @in_bytes += @msg_size
          @buf = @buf.slice((@msg_size + CR_LF_SIZE), @buf.bytesize)
          @msg_sub = @msg_size = @reply = nil
          @parse_state = AWAITING_CONTROL_LINE
          @buf = nil if (@buf && @buf.empty?)
        end
      end
    end

    def send_info
      send_data("INFO #{Server.info_string}#{CR_LF}")
    end

    def process_connect_config(config)
      @verbose  = config['verbose'] unless config['verbose'].nil?
      @pedantic = config['pedantic'] unless config['pedantic'].nil?
      return send_data(OK) unless Server.auth_required?

      EM.cancel_timer(@auth_pending)
      if Server.auth_ok?(config['user'], config['pass'])
        send_data(OK) if @verbose
        @auth_pending = nil
      else
        error_close AUTH_FAILED
        debug "Authorization failed for client connection", cid
      end
    end

    def delete_subscriber(sub)
      ctrace('DELSUB OP', sub.subject, sub.qgroup, sub.sid) if NATSD::Server.trace_flag?
      Server.unsubscribe(sub)
      @subscriptions.delete(sub.sid)
    end

    def error_close(msg)
      send_data(msg)
      close_connection_after_writing
      @closing = true
    end

    def unbind
      debug "Client connection closed", client_info, cid
      Server.num_connections -= 1
      @subscriptions.each_value { |sub| Server.unsubscribe(sub) }
      EM.cancel_timer(@auth_pending) if @auth_pending
      @auth_pending = nil
      EM.cancel_timer(@ping_timer) if @ping_timer
      @ping_timer = nil

      @closing = true
    end

    def ctrace(*args)
      trace(args, "c: #{cid}")
    end

    def strip_op(op='')
      op.dup.sub(CR_LF, EMPTY)
    end
  end

end
