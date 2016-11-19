module NATS
  # NOTE: These are all announced to the server on CONNECT
  VERSION = "0.8.2".freeze
  LANG    = (RUBY_PLATFORM == 'java' ? 'jruby' : 'ruby').freeze
  PROTOCOL_VERSION = 1
end
