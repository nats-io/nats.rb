
spec = Gem::Specification.new do |s|
  s.name = 'nats'
  s.version = '0.1'
  s.date = '2010-10-30'
  s.summary = 'Simple Publish-Subscribe Messaging System'
  s.homepage = "http://github.com/derekcollison/nats"
  s.description = "A lightweight, fast, publish-subscribe messaging system."
  s.has_rdoc = false

  s.authors = ["Derek Collison"]
  s.email = ["derek.collison@gmail.com"]

  s.add_dependency('eventmachine', '>= 0.12.4')
  s.add_dependency('yajl-ruby', '>= 0.7.8')

  s.require_paths = ['lib']
  s.bindir = 'bin'
  s.executables = ['nats-server']

  s.files = [
    "COPYING",
    "nats.gemspec",
    "Rakefile",
    "bin/nats-server",
    "lib/nats/client.rb",
    "lib/nats/ext/bytesize.rb",
    "lib/nats/ext/em.rb",
    "lib/nats/ext/json.rb",
    "lib/nats/server.rb",
    "lib/nats/server/parser.rb",
    "lib/nats/server/sublist.rb"
  ]

end