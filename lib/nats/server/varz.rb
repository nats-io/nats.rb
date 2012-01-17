module NATSD #:nodoc: all

  class Varz
    def call(env)
      varz_json = JSON.pretty_generate(Server.update_varz) + "\n"
      hdrs = RACK_JSON_HDR.dup
      hdrs['Content-Length'] = varz_json.bytesize.to_s
      [200, hdrs, varz_json]
    end
  end

  class Server
    class << self

      def update_varz
        # Snapshot uptime
        @varz[:uptime] = uptime_string(Time.now - @varz[:start])

        # Grab current cpu and memory usage.
        rss, pcpu = `ps -o rss=,pcpu= -p #{Process.pid}`.split
        @varz[:mem] = rss.to_i
        @varz[:cpu] = pcpu.to_f
        @varz[:connections] = num_connections
        @varz[:in_msgs] = in_msgs
        @varz[:out_msgs] = out_msgs
        @varz[:in_bytes] = in_bytes
        @varz[:out_bytes] = out_bytes
        @varz[:routes] = num_routes
        @last_varz_update = Time.now.to_f
        varz
      end

    end
  end

end
