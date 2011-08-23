require 'spec_helper'
require 'nats/server/server'
require 'nats/server/const'

describe 'server log and pid files' do

  before(:all) do
    LOG_SERVER_PID = '/tmp/nats_log_pid.pid'
    LOG_SERVER = 'nats://localhost:9299'
    LOG_LOG_FILE = '/tmp/nats_log_test.log'
    LOG_FLAGS = "-l #{LOG_LOG_FILE}"

    FileUtils.rm_f(LOG_LOG_FILE)
    @s = NatsServerControl.new(LOG_SERVER, LOG_SERVER_PID, LOG_FLAGS)
    @s.start_server
  end

  after(:all) do
    @s.kill_server
    NATS.server_running?(LOG_SERVER).should be_false
    FileUtils.rm_f(LOG_LOG_FILE)
  end

  it 'should create the log file' do
    File.exists?(LOG_LOG_FILE).should be_true
  end

  it 'should create the pid file' do
    File.exists?(LOG_SERVER_PID).should be_true
  end

  it 'should not leave a daemonized pid file in current directory' do
    File.exists?("./#{NATSD::APP_NAME}.pid").should be_false
  end

  it 'should append to the log file after restart' do
    @s.kill_server
    @s.start_server
    File.read(LOG_LOG_FILE).split("\n").size.should == 2
  end

end
