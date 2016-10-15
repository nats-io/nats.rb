require File.expand_path('../lib/nats/io/version', __FILE__)

spec = Gem::Specification.new do |s|
  s.name = 'nats-pure'
  s.version = NATS::IO::VERSION
  s.summary = 'NATS is an open-source, high-performance, lightweight cloud messaging system.'
  s.homepage = 'https://nats.io'
  s.description = 'NATS is an open-source, high-performance, lightweight cloud messaging system.'
  s.licenses = ['MIT']
  s.has_rdoc = false

  s.authors = ['Waldemar Quevedo']
  s.email = ['wally@apcera.com']

  s.require_paths = ['lib']

  s.files = %w[
    lib/nats/io/client.rb
    lib/nats/io/parser.rb
    lib/nats/io/version.rb
  ]
end
