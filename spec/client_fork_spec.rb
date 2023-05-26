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

return unless Process.respond_to?(:fork) # Skip if fork is not supported (Windows, JRuby, etc)
return unless Process.respond_to?(:_fork) # Skip if around fork callbacks are not supported (before Ruby 3.1)

require 'spec_helper'

describe 'Client - Fork detection' do

  before(:all) do
    @tmpdir = Dir.mktmpdir("ruby-jetstream-fork")
    @s = NatsServerControl.new("nats://127.0.0.1:4524", "/tmp/test-nats.pid", "-js -sd=#{@tmpdir}")
    @s.start_server(true)
  end

  after(:all) do
    @s.kill_server
    FileUtils.remove_entry(@tmpdir)
  end

  class Component
    attr_reader  :nats, :options
    attr_accessor :msgs

    def initialize(options={})
      @nats = NATS.connect("nats://127.0.0.1:4524", options)
      @msgs = []
    end
  end

  let(:options) { {} }
  let!(:component) { Component.new(options) }

  it 'should be able to publish messages from child process after forking' do
    received = nil
    component.nats.subscribe("forked-topic") do |msg|
      received = msg.data
    end

    pid = fork do
      component.nats.publish("forked-topic", "hey from the child process")
      component.nats.flush
    end
    Process.wait(pid)
    expect($?.exitstatus).to be_zero
    expect(received).to eq("hey from the child process")
    component.nats.close
  end

  it 'should be able to make requests messages from child process after forking' do
    received = nil
    component.nats.subscribe("service") do |msg|
      msg.respond("pong")
    end

    resp = component.nats.request("service", "ping")
    expect(resp.data).to eq("pong")

    pid = fork do
      resp = component.nats.request("service", "ping")
      expect(resp.data).to eq("pong")
    end
    Process.wait(pid)
    expect($?.exitstatus).to be_zero
    component.nats.close
  end

  it 'should be able to receive messages from child process after forking' do
    from_child, to_parent = IO.pipe
    from_parent, to_child = IO.pipe

    pid = fork do # child process
      to_child.close; from_child.close # close unused ends

      component.nats.subscribe("forked-topic") do |msg|
        to_parent.write(msg.data)
      end
      component.nats.flush

      to_parent.puts("proceed")

      from_parent.gets
      to_parent.close; from_parent.close
    end

    # parent process
    to_parent.close; from_parent.close # close unused ends

    from_child.gets
    component.nats.publish("forked-topic", "hey from the parent process")
    component.nats.flush

    to_child.puts("proceed")

    result = from_child.read
    expect(result).to eq("hey from the parent process")

    to_child.close; from_child.close
    Process.wait(pid)
  end

  it "should be able to use jetstreams from child process after forking" do
    js = component.nats.jetstream
    js.add_stream(name: "forked-stream", subjects: ["foo"])

    from_child, to_parent = IO.pipe

    pid = fork do # child process
      from_child.close # close unused ends

      psub = js.pull_subscribe("foo", "bar")
      msgs = psub.fetch(1)
      msgs.each(&:ack)

      to_parent.write(msgs.first.data)
    end
    to_parent.close

    js.publish("foo", "Hey JetStream!")

    result = from_child.read
    expect(result).to eq("Hey JetStream!")

    from_child.close
    Process.wait(pid)
  end

  context "when reconnection is disabled" do
    let(:options) { { reconnect: false } }

    it "raises an error in child process after fork is detected" do
      # FIXME: These commands should have raise NATS::IO::ConnectionClosed error,
      # but using a timeout instead for now for the assertion.
      expect do
        pid = fork do
          expect(component.nats.closed?).to eql(true)
          # FIXME: Raise ConnectionClosed when appropriate.
          # component.nats.publish("topic", "whatever")
          component.nats.flush(2)
        end
        Process.wait(pid)
        expect($?.exitstatus).to be_nonzero # child process should exit with error
      end.to output(/NATS::IO::Timeout/).to_stderr_from_any_process
      expect(component.nats.closed?).to eql(false)
      component.nats.close

      # expect do
      #   pid = fork do
      #     component.nats.publish("topic", "whatever")
      #     component.nats.flush
      #   end
      #   Process.wait(pid)
      #   expect($?.exitstatus).to be_nonzero # child process should exit with error
      # end.to output(/NATS::IO::ForkDetectedError/).to_stderr_from_any_process
    end
  end
end
