# Copyright 2021 The NATS Authors
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
require 'nats/io/client'
require 'nats/nuid'

# A thread safe Ruby client for the NATS messaging system (https://nats.io).
#
# @example Service, request/response example
#   nc = NATS.connect("demo.nats.io")
#   nc.subscribe("foo") do |msg|
#     msg.respond("Hello World")
#   end
#
#   resp = nc.request("foo")
#   puts "Received: #{msg.data}"
# 
# 
# @example Stream with an iterator example
#   nc = NATS.connect("demo.nats.io")
#   sub = nc.subscribe("foo")
# 
#   nc.publish("foo")
#   msg = sub.next_msg
#   puts "Received: #{msg.data}"
# 
module NATS
end
