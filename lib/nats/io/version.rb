module NATS
  module IO
    # NOTE: These are all announced to the server on CONNECT
    VERSION  = "0.3.0"
    LANG     = "#{RUBY_ENGINE}2".freeze
    PROTOCOL = 1
  end
end
