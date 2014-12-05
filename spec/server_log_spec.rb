require 'spec_helper'
require 'syslog'
require 'nats/server/server'
require 'nats/server/const'
require 'nats/server/options'
require 'nats/server/util'

describe 'server log and pid files' do

  before(:all) do
    LOG_SERVER_PID = '/tmp/nats_log_pid.pid'
    LOG_SERVER = 'nats://localhost:9299'
    LOG_LOG_FILE = '/tmp/nats_log_test.log'
    LOG_FLAGS = "-l #{LOG_LOG_FILE}"
    SYSLOG_IDENT = "nats_syslog_test"
    LOG_SYSLOG_FLAGS= "#{LOG_FLAGS} -S #{SYSLOG_IDENT}"

    FileUtils.rm_f(LOG_LOG_FILE)
    @s = NatsServerControl.new(LOG_SERVER, LOG_SERVER_PID, LOG_FLAGS)
    @s.start_server
  end

  after(:all) do
    @s.kill_server
    NATS.server_running?(LOG_SERVER).should be_falsey
    FileUtils.rm_f(LOG_LOG_FILE)
  end

  it 'should create the log file' do
    File.exists?(LOG_LOG_FILE).should be_truthy
  end

  it 'should create the pid file' do
    File.exists?(LOG_SERVER_PID).should be_truthy
  end

  it 'should not leave a daemonized pid file in current directory' do
    File.exists?("./#{NATSD::APP_NAME}.pid").should be_falsey
  end

  it 'should append to the log file after restart' do
    @s.kill_server
    @s.start_server
    File.read(LOG_LOG_FILE).split("\n").size.should == 2
  end

  it 'should not output to the log file when enable syslog option' do
    @s.kill_server
    FileUtils.rm_f(LOG_LOG_FILE)
    @s = NatsServerControl.new(LOG_SERVER, LOG_SERVER_PID, LOG_SYSLOG_FLAGS)
    @s.start_server
    File.read(LOG_LOG_FILE).split("\n").size.should == 0
  end

 it 'should use Syslog module methods when enable syslog option' do
    *log_msg  = 'syslog test'
    Syslog.should_receive(:open).with(SYSLOG_IDENT, Syslog::LOG_PID, Syslog::LOG_USER)
    Syslog.should_receive(:log).with(Syslog::LOG_NOTICE, '%s', PP::pp(log_msg, '', 120))
    Syslog.should_receive(:close)
    NATSD::Server.process_options(LOG_SYSLOG_FLAGS.split)
    NATSD::Server.open_syslog
    log  *log_msg
    NATSD::Server.close_syslog
 end

end
