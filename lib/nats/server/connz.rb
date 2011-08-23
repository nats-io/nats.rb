module NATSD #:nodoc: all

  class Connz
    def call(env)
      connz_json = JSON.pretty_generate(Server.dump_connections) + "\n"
      hdrs = RACK_JSON_HDR.dup
      hdrs['Content-Length'] = connz_json.bytesize.to_s
      [200, hdrs, connz_json]
    end
  end

  class Server
    class << self

      def dump_connections
        conns = []
        ObjectSpace.each_object(NATSD::Connection) do |c|
          conns << c.info unless c.closing?
        end
        { :connections => conns }
      end

    end
  end

end
