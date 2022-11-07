# Copyright 2016-2022 The NATS Authors
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

require_relative 'lib/nats/io/version'

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
    lib/nats.rb
    lib/nats/client.rb
    lib/nats/io/client.rb
    lib/nats/io/errors.rb
    lib/nats/io/jetstream.rb
    lib/nats/io/jetstream/api.rb
    lib/nats/io/jetstream/errors.rb
    lib/nats/io/jetstream/js.rb
    lib/nats/io/jetstream/js/config.rb
    lib/nats/io/jetstream/js/header.rb
    lib/nats/io/jetstream/js/status.rb
    lib/nats/io/jetstream/js/sub.rb
    lib/nats/io/jetstream/manager.rb
    lib/nats/io/jetstream/msg.rb
    lib/nats/io/jetstream/msg/ack.rb
    lib/nats/io/jetstream/msg/ack_methods.rb
    lib/nats/io/jetstream/msg/metadata.rb
    lib/nats/io/jetstream/pull_subscription.rb
    lib/nats/io/jetstream/push_subscription.rb
    lib/nats/io/kv.rb
    lib/nats/io/kv/api.rb
    lib/nats/io/kv/bucket_status.rb
    lib/nats/io/kv/errors.rb
    lib/nats/io/kv/manager.rb
    lib/nats/io/msg.rb
    lib/nats/io/parser.rb
    lib/nats/io/subscription.rb
    lib/nats/io/version.rb
    lib/nats/nuid.rb
  ]
end
