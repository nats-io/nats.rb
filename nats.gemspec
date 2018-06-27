# Copyright 2010-2018 The NATS Authors
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

lib = File.expand_path('../lib/', __FILE__)
$:.unshift lib unless $:.include?(lib)

require 'nats/version'

spec = Gem::Specification.new do |s|
  s.name = 'nats'
  s.version = NATS::VERSION
  s.summary = 'NATS is an open-source, high-performance, lightweight cloud messaging system.'
  s.homepage = 'https://nats.io'
  s.description = 'NATS is an open-source, high-performance, lightweight cloud messaging system.'
  s.licenses = ['MIT']

  s.authors = ['Derek Collison']
  s.email = ['derek.collison@gmail.com']
  s.add_dependency('eventmachine', '~> 1.2', '>= 1.2')

  s.require_paths = ['lib']
  s.bindir = 'bin'
  s.executables = ['nats-pub', 'nats-sub', 'nats-queue', 'nats-request']

  s.files = %w[
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
    lib/nats/nuid.rb
    lib/nats/version.rb
    lib/nats/ext/bytesize.rb
    lib/nats/ext/em.rb
    lib/nats/ext/json.rb
    lib/nats/server.rb
    lib/nats/server/server.rb
    lib/nats/server/connection.rb
    lib/nats/server/cluster.rb
    lib/nats/server/route.rb
    lib/nats/server/options.rb
    lib/nats/server/sublist.rb
    lib/nats/server/const.rb
    lib/nats/server/util.rb
    lib/nats/server/varz.rb
    lib/nats/server/connz.rb
  ]

end
