module NATS
  module IO
    # NOTE: These are all announced to the server on CONNECT
    VERSION = "0.1.0"
    LANG    = (RUBY_PLATFORM == 'java' ? 'jruby2' : 'ruby2').freeze
  end
end
