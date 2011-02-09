begin
  require 'eventmachine'
rescue LoadError
  require 'rubygems'
  require 'eventmachine'
end

# Check for get_outbound_data_size support, fake it out if it doesn't exist, e.g. jruby
if !EM::Connection.method_defined? :get_outbound_data_size
  class EM::Connection
    def get_outbound_data_size; return 0; end
  end
end
