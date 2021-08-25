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

require 'spec_helper'
require 'fileutils'

describe 'Client - Authorization' do

  USER = 'secret'
  PASS = 'password'

  TEST_AUTH_SERVER          = "nats://#{USER}:#{PASS}@127.0.0.1:9222"
  TEST_AUTH_SERVER_PID      = '/tmp/nats_authorization.pid'
  TEST_AUTH_SERVER_NO_CRED  = 'nats://127.0.0.1:9222'

  TEST_ANOTHER_AUTH_SERVER  = "nats://#{USER}:secret@127.0.0.1:9223"
  TEST_ANOTHER_AUTH_SERVER_PID = '/tmp/nats_another_authorization.pid'

  TEST_TOKEN_AUTH_SERVER = "nats://#{USER}@127.0.0.1:9222"

  after (:each) do
    @server_control.kill_server
    FileUtils.rm_f TEST_AUTH_SERVER_PID
  end

  it 'should connect to an authorized server with proper credentials' do
    @server_control = NatsServerControl.new(TEST_AUTH_SERVER, TEST_AUTH_SERVER_PID)
    @server_control.start_server
    nats = NATS::IO::Client.new
    expect do
      nats.connect(:servers => [TEST_AUTH_SERVER], :reconnect => false)
      nats.flush
    end.to_not raise_error
    nats.close
  end

  it 'should connect to an authorized server with token' do
    @server_control = NatsServerControl.new(TEST_TOKEN_AUTH_SERVER, TEST_AUTH_SERVER_PID)
    @server_control.start_server
    nats = NATS::IO::Client.new
    expect do
      nats.connect(:servers => [TEST_TOKEN_AUTH_SERVER], :reconnect => false)
      nats.flush
    end.to_not raise_error


    expect do
      nc = NATS.connect(TEST_TOKEN_AUTH_SERVER, reconnect: false)
      nc.flush
    end.to_not raise_error

    nats.close
  end

  it 'should fail to connect to an authorized server without proper credentials' do
    @server_control = NatsServerControl.new(TEST_AUTH_SERVER, TEST_AUTH_SERVER_PID)
    @server_control.start_server
    nats = NATS::IO::Client.new
    errors = []
    disconnect_errors = []
    expect do
      nats.on_disconnect do |e|
        disconnect_errors << e
      end
      nats.on_error do |e|
        errors << e
      end
      nats.connect({
        servers: [TEST_AUTH_SERVER_NO_CRED],
        reconnect: false
      })
    end.to raise_error(NATS::IO::AuthError)
    expect(errors.count).to eql(1)
    expect(errors.first).to be_a(NATS::IO::AuthError)
    expect(disconnect_errors.count).to eql(1)
    expect(disconnect_errors.first).to be_a(NATS::IO::AuthError)
  end
end
