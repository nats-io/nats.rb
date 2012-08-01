require 'uri'

module NATSD #:nodoc: all

  class Server
    class << self
      attr_reader :opt_routes, :route_auth_required, :route_ssl_required, :reconnect_interval
      attr_accessor :num_routes

      alias route_auth_required? :route_auth_required
      alias route_ssl_required? :route_ssl_required

      def connected_routes
        @routes ||= []
      end

      def add_route(route)
        connected_routes << route unless route.nil?
      end

      def remove_route(route)
        connected_routes.delete(route) unless route.nil?
      end

      def route_info_string
        @route_info = {
          :server_id => Server.id,
          :host => @options[:cluster_net] || host,
          :port => @options[:cluster_port],
          :version => VERSION,
          :auth_required => route_auth_required?,
          :ssl_required => false,                 # FIXME!
          :max_payload => @max_payload
        }
        @route_info.to_json
      end

      def route_key(route_url)
        r = URI.parse(route_url)
        "#{r.host}:#{r.port}"
      end

      def route_auth_ok?(user, pass)
        user == @options[:cluster_user] && pass == @options[:cluster_pass]
      end

      def solicit_routes #:nodoc:
        @opt_routes = []
        NATSD::Server.options[:cluster_routes].each do |r_url|
          opt_routes << { :route => r_url, :uri => URI.parse(r_url), :key => route_key(r_url) }
        end
        try_to_connect_routes
      end

      def try_to_connect_routes #:nodoc:
        opt_routes.each do |route|
          # FIXME, Strip auth
          debug "Trying to connect to route: #{route[:route]}"
          EM.connect(route[:uri].host, route[:uri].port, NATSD::Route, route)
        end
      end

      def broadcast_proto_to_routes(proto)
        connected_routes.each { |r| r.queue_data(proto) }
      end

      def rsid_qsub(rsid)
        cid, sid = parse_rsid(rsid)
        conn = Server.connections[cid]
        sub = conn.subscriptions[sid]
        sub if sub.qgroup
      rescue
        nil
      end

      def parse_rsid(rsid)
        m = RSID.match(rsid)
        return [m[1].to_i, m[2]] if m
      end

      def routed_sid(sub)
        "RSID:#{sub.conn.cid}:#{sub.sid}"
      end

      def route_sub_proto(sub)
        return "SUB #{sub.subject} #{routed_sid(sub)}#{CR_LF}" if sub.qgroup.nil?
        return "SUB #{sub.subject} #{sub.qgroup} #{routed_sid(sub)}#{CR_LF}"
      end

      def broadcast_sub_to_routes(sub)
        broadcast_proto_to_routes(route_sub_proto(sub))
      end

      def broadcast_unsub_to_routes(sub)
        opt_max_str = " #{sub.max_responses}" unless sub.max_responses.nil?
        broadcast_proto_to_routes("UNSUB #{routed_sid(sub)}#{opt_max_str}#{CR_LF}")
      end

    end
  end

end
