module NATSD #:nodoc: all

  # Need to make this a class with EM > 1.0
  class Route < EventMachine::Connection #:nodoc:

    include Connection

    attr_reader :rid, :remote_rid, :closing, :r_obj, :reconnecting
    alias :peer_info :client_info
    alias :reconnecting? :reconnecting

    def initialize(route=nil)
      @r_obj = route
    end

    def solicited?
      r_obj != nil
    end

    def connection_completed
      debug "Route connected", rid
      return unless reconnecting?

      # Kill reconnect if we got here from there
      cancel_reconnect
      @buf, @closing = nil, false
      post_init
    end

    def post_init
      @rid = Server.rid
      @subscriptions = {}
      @in_msgs = @out_msgs = @in_bytes = @out_bytes = 0
      @writev_size = 0
      @parse_state = AWAITING_CONTROL_LINE

      # Queue up auth if needed and we solicited the connection
      debug "Route connection created", peer_info, rid

      # queue up auth if needed and we solicited the connection
      if solicited?
        debug "Route sent authorization", rid
        send_auth
      else
        # FIXME, separate variables for timeout?
        @auth_pending = EM.add_timer(NATSD::Server.auth_timeout) { connect_auth_timeout } if Server.route_auth_required?
      end

      send_info
      @ping_timer = EM.add_periodic_timer(NATSD::Server.ping_interval) { send_ping }
      @pings_outstanding = 0
      inc_connections
      send_local_subs_to_route
    end

    # TODO: Make sure max_requested is also propogated on reconnect
    def send_local_subs_to_route
      ObjectSpace.each_object(NATSD::Connection) do |c|
        next if c.closing? || c.type != 'Client'
        c.subscriptions.each_value do |sub|
          queue_data(NATSD::Server.route_sub_proto(sub))
        end
      end
    end

    def process_connect_route_config(config)
      @verbose  = config['verbose'] unless config['verbose'].nil?
      @pedantic = config['pedantic'] unless config['pedantic'].nil?

      return queue_data(OK) unless Server.route_auth_required?

      EM.cancel_timer(@auth_pending)
      if auth_ok?(config['user'], config['pass'])
        debug "Route received proper credentials", rid
        queue_data(OK) if @verbose
        @auth_pending = nil
      else
        error_close AUTH_FAILED
        debug "Authorization failed for #{type.downcase} connection", rid
      end
    end

    def connect_auth_timeout
      error_close AUTH_REQUIRED
      debug "#{type} connection timeout due to lack of auth credentials", rid
    end

    def receive_data(data)
      @buf = @buf ? @buf << data : data
      return close_connection if @buf =~ /(\006|\004)/ # ctrl+c or ctrl+d for telnet friendly

      while (@buf && !@closing)
        case @parse_state
        when AWAITING_CONTROL_LINE
          case @buf
          when MSG
            ctrace('MSG OP', strip_op($&)) if NATSD::Server.trace_flag?
            return connect_auth_timeout if @auth_pending
            @buf = $'
            @parse_state = AWAITING_MSG_PAYLOAD
            @msg_sub, @msg_sid, @msg_reply, @msg_size = $1, $2, $4, $5.to_i
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
            Server.subscribe(sub, is_route?)
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
              process_connect_route_config(config)
            rescue => e
              queue_data(INVALID_CONFIG)
              log_error
            end
          when INFO_REQ
            ctrace('INFO_REQUEST OP') if NATSD::Server.trace_flag?
            return connect_auth_timeout if @auth_pending
            @buf = $'
            send_info
          when INFO
            ctrace('INFO OP', strip_op($&)) if NATSD::Server.trace_flag?
            return connect_auth_timeout if @auth_pending
            @buf = $'
            process_info($1)
          when ERR_RESP
            ctrace('-ERR', $1) if NATSD::Server.trace_flag?
            close_connection
            exit
          when OK_RESP
            ctrace('+OK') if NATSD::Server.trace_flag?
            @buf = $'
          when UNKNOWN
            ctrace('Unknown Op', strip_op($&)) if NATSD::Server.trace_flag?
            return connect_auth_timeout if @auth_pending
            @buf = $'
            queue_data(UNKNOWN_OP)
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

          ctrace('Processing routed msg', @msg_sub, @msg_reply, msg) if NATSD::Server.trace_flag?
          queue_data(OK) if @verbose

          # We deliver normal subscriptions like a client publish, which
          # eliminates the duplicate traversal over the route. However,
          # qgroups are sent individually per group for only the route
          # with the intended subscriber, since route interest is L2
          # semantics, we deliver those direct.
          if (sub = Server.rsid_qsub(@msg_sid))
            # Allows nil reply to not have extra space
            reply = @msg_reply + ' ' if @msg_reply
            Server.deliver_to_subscriber(sub, @msg_sub, reply, msg)
          else
            Server.route_to_subscribers(@msg_sub, @msg_reply, msg, is_route?)
          end

          @in_msgs += 1
          @in_bytes += @msg_size
          @buf = @buf.slice((@msg_size + CR_LF_SIZE), @buf.bytesize)
          @msg_sub = @msg_size = @reply = nil
          @parse_state = AWAITING_CONTROL_LINE
          @buf = nil if (@buf && @buf.empty?)
        end
      end
    end

    def send_auth
      return unless r_obj[:uri].user
      cs = { :user => r_obj[:uri].user, :pass => r_obj[:uri].password }
      queue_data("CONNECT #{cs.to_json}#{CR_LF}")
    end

    def send_info
      queue_data("INFO #{Server.route_info_string}#{CR_LF}")
    end

    def process_info(info_json)
      info = JSON.parse(info_json)
      @remote_rid = info['server_id'] unless info['server_id'].nil?
      super(info_json)
    end

    def auth_ok?(user, pass)
      Server.route_auth_ok?(user, pass)
    end

    def inc_connections
      Server.num_routes += 1
      Server.add_route(self)
    end

    def dec_connections
      Server.num_routes -= 1
      Server.remove_route(self)
    end

    def try_reconnect
      debug "Trying to reconnect route", peer_info, rid
      EM.reconnect(r_obj[:uri].host, r_obj[:uri].port, self)
    end

    def cancel_reconnect
      EM.cancel_timer(@reconnect_timer) if @reconnect_timer
      @reconnect_timer = nil
      @reconnecting = false
    end

    def unbind
      return if reconnecting?
      debug "Route connection closed", peer_info, rid
      process_unbind
      if solicited?
        @reconnecting = true
        @reconnect_timer = EM.add_periodic_timer(NATSD::DEFAULT_ROUTE_RECONNECT_INTERVAL) { try_reconnect }
      end
    end

    def ctrace(*args)
      trace(args, "r: #{rid}")
    end

    def is_route?
      true
    end

    def type
      'Route'
    end

  end

end
