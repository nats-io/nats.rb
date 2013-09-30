module NATSD #:nodoc: all

  module Connection #:nodoc: all

    attr_accessor :in_msgs, :out_msgs, :in_bytes, :out_bytes
    attr_reader :cid, :closing, :last_activity, :writev_size, :subscriptions
    alias :closing? :closing

    def flush_data
      return if @writev.nil? || closing?
      send_data(@writev.join)
      @writev, @writev_size = nil, 0
    end

    def queue_data(data)
      EM.next_tick { flush_data } if @writev.nil?
      (@writev ||= []) << data
      @writev_size += data.bytesize
      flush_data if @writev_size > MAX_WRITEV_SIZE
    end

    def client_info
      @client_info ||= (get_peername.nil? ? 'N/A' : Socket.unpack_sockaddr_in(get_peername))
    end

    def info
      {
        :cid => cid,
        :ip => client_info[1],
        :port => client_info[0],
        :subscriptions => @subscriptions.size,
        :pending_size => get_outbound_data_size,
        :in_msgs => @in_msgs,
        :out_msgs => @out_msgs,
        :in_bytes => @in_bytes,
        :out_bytes => @out_bytes
      }
    end

    def max_connections_exceeded?
      return false unless (Server.num_connections > Server.max_connections)
      error_close MAX_CONNS_EXCEEDED
      debug "Maximum #{Server.max_connections} connections exceeded, c:#{cid} will be closed"
      true
    end

    def post_init
      @cid = Server.cid
      @subscriptions = {}
      @verbose = @pedantic = true # suppressed by most clients, but allows friendly telnet
      @in_msgs = @out_msgs = @in_bytes = @out_bytes = 0
      @writev_size = 0
      @parse_state = AWAITING_CONTROL_LINE
      send_info
      debug "#{type} connection created", client_info, cid
      if Server.ssl_required?
        debug "Starting TLS/SSL", client_info, cid
        flush_data
        @ssl_pending = EM.add_timer(NATSD::Server.ssl_timeout) { connect_ssl_timeout }
        start_tls(:verify_peer => true) if Server.ssl_required?
      end
      @auth_pending = EM.add_timer(NATSD::Server.auth_timeout) { connect_auth_timeout } if Server.auth_required?
      @ping_timer = EM.add_periodic_timer(NATSD::Server.ping_interval) { send_ping }
      @pings_outstanding = 0
      inc_connections
      return if max_connections_exceeded?
    end

    def send_ping
      return if @closing
      if @pings_outstanding > NATSD::Server.ping_max
        error_close UNRESPONSIVE
        return
      end
      queue_data(PING_RESPONSE)
      flush_data
      @pings_outstanding += 1
    end

    def connect_auth_timeout
      error_close AUTH_REQUIRED
      debug "#{type} connection timeout due to lack of auth credentials", cid
    end

    def connect_ssl_timeout
      error_close SSL_REQUIRED
      debug "#{type} connection timeout due to lack of TLS/SSL negotiations", cid
    end

    def receive_data(data)
      @buf = @buf ? @buf << data : data

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
              debug_print_msg_too_big(@msg_size)
              error_close PAYLOAD_TOO_BIG
            end
            queue_data(INVALID_SUBJECT) if (@pedantic && !(@msg_sub =~ SUB_NO_WC))
          when SUB_OP
            ctrace('SUB OP', strip_op($&)) if NATSD::Server.trace_flag?
            return connect_auth_timeout if @auth_pending
            @buf = $'
            sub, qgroup, sid = $1, $3, $4
            return queue_data(INVALID_SUBJECT) if !($1 =~ SUB)
            return queue_data(INVALID_SID_TAKEN) if @subscriptions[sid]
            sub = Subscriber.new(self, sub, sid, qgroup, 0)
            @subscriptions[sid] = sub
            Server.subscribe(sub)
            queue_data(OK) if @verbose
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
              queue_data(OK) if @verbose
            else
              queue_data(INVALID_SID_NOEXIST) if @pedantic
            end
          when PING
            ctrace('PING OP') if NATSD::Server.trace_flag?
            @buf = $'
            queue_data(PONG_RESPONSE)
            flush_data
          when PONG
            ctrace('PONG OP') if NATSD::Server.trace_flag?
            @buf = $'
            @pings_outstanding -= 1
          when CONNECT
            ctrace('CONNECT OP', strip_op($&)) if NATSD::Server.trace_flag?
            @buf = $'
            begin
              config = JSON.parse($1)
              process_connect_config(config)
            rescue => e
              queue_data(INVALID_CONFIG)
              log_error
            end
          when INFO_REQ
            ctrace('INFO_REQUEST OP') if NATSD::Server.trace_flag?
            return connect_auth_timeout if @auth_pending
            @buf = $'
            send_info
          when UNKNOWN
            ctrace('Unknown Op', strip_op($&)) if NATSD::Server.trace_flag?
            return connect_auth_timeout if @auth_pending
            @buf = $'
            queue_data(UNKNOWN_OP)
          when CTRL_C # ctrl+c or ctrl+d for telnet friendly
            ctrace('CTRL-C encountered', strip_op($&)) if NATSD::Server.trace_flag?
            return close_connection
          when CTRL_D # ctrl+d for telnet friendly
            ctrace('CTRL-D encountered', strip_op($&)) if NATSD::Server.trace_flag?
            return close_connection
          else
            # If we are here we do not have a complete line yet that we understand.
            # If too big, cut the connection off.
            if @buf.bytesize > NATSD::Server.max_control_line
              debug_print_controlline_too_big(@buf.bytesize)
              close_connection
            end
            return
          end
          @buf = nil if (@buf && @buf.empty?)

        when AWAITING_MSG_PAYLOAD
          return unless (@buf.bytesize >= (@msg_size + CR_LF_SIZE))
          msg = @buf.slice(0, @msg_size)
          ctrace('Processing msg', @msg_sub, @msg_reply, msg) if NATSD::Server.trace_flag?
          queue_data(OK) if @verbose
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
      queue_data("INFO #{Server.info_string}#{CR_LF}")
    end

    # Placeholder
    def process_info(info)
    end

    def auth_ok?(user, pass)
      Server.auth_ok?(user, pass)
    end

    def process_connect_config(config)
      @verbose  = config['verbose'] unless config['verbose'].nil?
      @pedantic = config['pedantic'] unless config['pedantic'].nil?

      return queue_data(AUTH_OK) unless Server.auth_required?

      EM.cancel_timer(@auth_pending)
      if auth_ok?(config['user'], config['pass'])
        queue_data(AUTH_OK)
        @auth_pending = nil
      else
        error_close AUTH_FAILED
        debug "Authorization failed for #{type.downcase} connection", cid
      end
    end

    def delete_subscriber(sub)
      ctrace('DELSUB OP', sub.subject, sub.qgroup, sub.sid) if NATSD::Server.trace_flag?
      Server.unsubscribe(sub, is_route?)
      @subscriptions.delete(sub.sid)
    end

    def error_close(msg)
      queue_data(msg)
      flush_data
      EM.next_tick { close_connection_after_writing }
      @closing = true
    end

    def debug_print_controlline_too_big(line_size)
      sizes = "#{pretty_size(line_size)} vs #{pretty_size(NATSD::Server.max_control_line)} max"
      debug "Control line size exceeded (#{sizes}), closing connection.."
    end

    def debug_print_msg_too_big(msg_size)
      sizes = "#{pretty_size(msg_size)} vs #{pretty_size(NATSD::Server.max_payload)} max"
      debug "Message payload size exceeded (#{sizes}), closing connection"
    end

    def inc_connections
      Server.num_connections += 1
      Server.connections[cid] = self
    end

    def dec_connections
      Server.num_connections -= 1
      Server.connections.delete(cid)
    end

    def process_unbind
      dec_connections
      EM.cancel_timer(@ssl_pending) if @ssl_pending
      @ssl_pending = nil
      EM.cancel_timer(@auth_pending) if @auth_pending
      @auth_pending = nil
      EM.cancel_timer(@ping_timer) if @ping_timer
      @ping_timer = nil
      @subscriptions.each_value { |sub| Server.unsubscribe(sub) }
      @closing = true
    end

    def unbind
      debug "Client connection closed", client_info, cid
      process_unbind
    end

    def ssl_handshake_completed
      EM.cancel_timer(@ssl_pending)
      @ssl_pending = nil
      cert = get_peer_cert
      debug "#{type} Certificate:", cert ? cert : 'N/A', cid
    end

    # FIXME! Cert accepted by default
    def ssl_verify_peer(cert)
      true
    end

    def ctrace(*args)
      trace(args, "c: #{cid}")
    end

    def strip_op(op='')
      op.dup.sub(CR_LF, EMPTY)
    end

    def is_route?
      false
    end

    def type
      'Client'
    end

  end

end
