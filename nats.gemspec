

lib = File.expand_path('../lib/', __FILE__)
$:.unshift lib unless $:.include?(lib)

require 'nats/server/const'

spec = Gem::Specification.new do |s|
  s.name = 'nats'
  s.version = NATSD::VERSION
  s.summary = 'A lightweight cloud messaging system.'
  s.homepage = 'http://github.com/derekcollison/nats'
  s.description = 'A lightweight cloud messaging system.'
  s.has_rdoc = true

  s.authors = ['Derek Collison']
  s.email = ['derek.collison@gmail.com']

  s.add_dependency('eventmachine', '= 1.0.3')
  s.add_dependency('json_pure', '>= 1.7.3')
  s.add_dependency('daemons', '>= 1.1.5')
  s.add_dependency('thin', '>= 1.4.1', '< 1.6')

  s.require_paths = ['lib']
  s.bindir = 'bin'
  s.executables = [NATSD::APP_NAME, 'nats-pub', 'nats-sub', 'nats-queue', 'nats-top', 'nats-request']

  s.files = %w[
    COPYING
    README.md
    HISTORY.md
    nats.gemspec
    Rakefile
    bin/nats-server
    bin/nats-sub
    bin/nats-pub
    bin/nats-queue
    bin/nats-top
    bin/nats-request
    lib/nats/client.rb
    lib/nats/ext/bytesize.rb
    lib/nats/ext/em.rb
    lib/nats/ext/json.rb
    lib/nats/server.rb
    lib/nats/server/server.rb
    lib/nats/server/connection.rb
    lib/nats/server/options.rb
    lib/nats/server/sublist.rb
    lib/nats/server/const.rb
    lib/nats/server/util.rb
    lib/nats/server/varz.rb
    lib/nats/server/connz.rb
  ]

end
