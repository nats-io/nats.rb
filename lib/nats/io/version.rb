# Copyright 2016-2021 The NATS Authors
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

module NATS
  module IO
    # VERSION is the version of the client announced on CONNECT to the server.
    VERSION = "2.0.0-rc2".freeze

    # LANG is the lang runtime of the client announced on CONNECT to the server.
    LANG = "#{RUBY_ENGINE}#{RUBY_VERSION}".freeze

    # PROTOCOL is the supported version of the protocol in the client.
    PROTOCOL = 1
  end
end
