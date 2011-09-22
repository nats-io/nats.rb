module NATSD #:nodoc: all

  class Connz
    def call(env)
      c_info = Server.dump_connections
      qs = env['QUERY_STRING']
      if (qs =~ /n=(\d)/)
        conns = c_info[:connections]
        c_info[:connections] = conns.sort { |a,b| b[:pending_size] <=> a[:pending_size] } [0, $1.to_i]
      end
      connz_json = JSON.pretty_generate(c_info) + "\n"
      hdrs = RACK_JSON_HDR.dup
      hdrs['Content-Length'] = connz_json.bytesize.to_s
      [200, hdrs, connz_json]
    end
  end

  class Server
    class << self

      def dump_connections
        conns, total = [], 0
        ObjectSpace.each_object(NATSD::Connection) do |c|
          next if c.closing?
          total += c.info[:pending_size]
          conns << c.info
        end
        {
          :pending_size => total,
          :num_connections => conns.size,
          :connections => conns
        }
      end

    end
  end

end
