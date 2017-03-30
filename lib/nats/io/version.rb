module NATS
  module IO
    # NOTE: These are all announced to the server on CONNECT
    VERSION  = "0.2.4"
    LANG     = "#{RUBY_ENGINE}2".freeze
    PROTOCOL = 1
  end
end
