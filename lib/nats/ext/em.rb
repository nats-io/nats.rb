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

begin
  require 'eventmachine'
rescue LoadError
  require 'rubygems'
  require 'eventmachine'
end

# Check for get_outbound_data_size support, fake it out if it doesn't exist, e.g. jruby
if !EM::Connection.method_defined? :get_outbound_data_size
  class EM::Connection
    def get_outbound_data_size; return 0; end
  end
end
