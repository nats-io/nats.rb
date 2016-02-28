
require 'spec_helper'
require 'fileutils'

require 'nats/server/server'
require 'nats/server/options'
require 'nats/server/const'
require 'nats/server/util'

describe 'server ping' do

  before (:all) do
    config_file = File.dirname(__FILE__) + '/resources/ping.yml'
    config = File.open(config_file) { |f| YAML.load(f) }
    NATSD::Server.process_options("-c #{config_file}".split)
    @opts = NATSD::Server.options
    @log_file = config['log_file']
    @host = config['net']
    @port = config['port']
    @uri = "nats://#{@host}:#{@port}"
    @s = NatsServerControl.new(@uri, config['pid_file'], "-c #{config_file}")
    @s.start_server
  end

  after(:all) do
    @s.kill_server
    FileUtils.rm_f(@log_file)
  end

  it 'should set default values for ping if not set' do
    config_file = File.dirname(__FILE__) + '/resources/config.yml'
    NATSD::Server.process_options("-c #{config_file}".split)
    opts = NATSD::Server.options
    opts[:ping_interval].should == 120
    opts[:ping_max].should == 2
  end

  it 'should properly parse ping parameters from config file' do
    @opts[:ping_interval].should == 0.1
    @opts[:ping_max].should == 2
  end

  it 'should ping us periodically' do
    NATS.start(:uri => @uri) do |connection|
      time_to_wait = @opts[:ping_interval] * @opts[:ping_max] + 0.2
      EM.add_timer(time_to_wait) do
        connection.pings.should >= @opts[:ping_max]
        NATS.stop
      end
    end
  end

  it 'should disconnect us when we do not respond' do
    begin
      s = TCPSocket.open(@host, @port)
      time_to_wait = @opts[:ping_interval] * (@opts[:ping_max] + 2)
      sleep(time_to_wait)
      buf = s.read(1024)
      buf.should =~ /PING/
      buf.should =~ /-ERR/
      buf.should =~ /Unresponsive client detected, connection dropped/
    ensure
      s.close if s
    end
  end

end
