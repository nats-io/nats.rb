require 'uri'

module NATSD #:nodoc: all

  class Server
    class << self
      attr_reader :opt_routes, :num_routes

      def connected_routes
        @routes ||= []
      end

      def route_key(route_url)
        r = URI.parse(route_url)
        "#{r.host}:#{r.port}"
      end

      def solicit_routes #:nodoc:
        @opt_routes = []
        NATSD::Server.options[:cluster_routes].each do |r_url|
          opt_routes << { :route => r_url, :uri => URI.parse(r_url), :key => route_key(r_url) }
        end
        try_to_connect_routes
      end

      def try_to_connect_routes #:nodoc:
        opt_routes.each do |r|
          debug "Trying to connect to route: #{r[:key]}"
          EM.connect(r[:uri].host, r[:uri].port, NATSD::Route, r)
        end
      end

    end
  end

end
