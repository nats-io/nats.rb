# Copyright 2016-2018 The NATS Authors
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

require File.expand_path('../lib/nats/io/version', __FILE__)

spec = Gem::Specification.new do |s|
  s.name = 'nats-pure'
  s.version = NATS::IO::VERSION
  s.summary = 'NATS is an open-source, high-performance, lightweight cloud messaging system.'
  s.homepage = 'https://nats.io'
  s.description = 'NATS is an open-source, high-performance, lightweight cloud messaging system.'
  s.licenses = ['Apache-2.0']

  s.authors = ['Waldemar Quevedo']
  s.email = ['wally@synadia.com']

  s.require_paths = ['lib']

  s.files = %w[
    lib/nats/io/client.rb
    lib/nats/io/parser.rb
    lib/nats/io/version.rb
    lib/nats/nuid.rb
  ]
end
