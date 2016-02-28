require 'spec_helper'
require 'fileutils'

describe 'server exit codes' do

  before (:all) do
    config_file = File.dirname(__FILE__) + '/resources/nonexistant.yml'
    uri = 'nats://localhost:4222'
    pid_file = '/tmp/test-nats-exit.pid'
    @s = NatsServerControl.new(uri, pid_file, "-c #{config_file}")
  end

  after(:all) do
    @s.kill_server
  end

  it 'should exit with non-zero status code when config file not found' do
    exit_code = @s.start_server(false)
    exit_code.should_not == 0
  end

end
