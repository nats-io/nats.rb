

lib = File.expand_path('../lib/', __FILE__)
$:.unshift lib unless $:.include?(lib)

require 'nats/server/const.rb'

spec = Gem::Specification.new do |s|
  s.name = 'nats'
  s.version = NATSD::VERSION
  s.date = '2011-2-2'
  s.summary = 'Simple PubSub Messaging System'
  s.homepage = "http://github.com/derekcollison/nats"
  s.description = "A lightweight publish-subscribe messaging system."
  s.has_rdoc = true

  s.authors = ["Derek Collison"]
  s.email = ["derek.collison@gmail.com"]

  s.add_dependency('eventmachine', '>= 0.12.10')
  s.add_dependency('yajl-ruby', '>= 0.8.0')
  s.add_dependency('daemons', '>= 1.1.0')

  s.require_paths = ['lib']
  s.bindir = 'bin'
  s.executables = [NATSD::APP_NAME, 'nats-pub', 'nats-sub', 'nats-queue']

  s.files = %w[
    COPYING
    README.md
    nats.gemspec
    Rakefile
    bin/nats-server
    bin/nats-sub
    bin/nats-pub
    bin/nats-queue
    lib/nats/client.rb
    lib/nats/ext/bytesize.rb
    lib/nats/ext/em.rb
    lib/nats/ext/json.rb
    lib/nats/server.rb
    lib/nats/server/options.rb
    lib/nats/server/sublist.rb
    lib/nats/server/const.rb
  ]

end