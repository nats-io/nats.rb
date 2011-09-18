
require 'spec_helper'
require 'fileutils'
require 'nats/server/server'
require "nats/server/sublist"
require "nats/server/options"
require "nats/server/const"
require "nats/server/util"

describe 'multi-user authorization' do

  before (:all) do
    config_file = File.dirname(__FILE__) + '/resources/multi_user_auth.yml'
    @config = File.open(config_file) { |f| YAML.load(f) }
    NATSD::Server.process_options("-c #{config_file}".split)
    @opts = NATSD::Server.options
    @log_file = @config['log_file']
    @host = @config['net']
    @port = @config['port']
    @uri = "nats://#{@host}:#{@port}"
    @s = NatsServerControl.new(@uri, @config['pid_file'], "-c #{config_file}")
    @s.start_server
  end

  after (:all) do
    @s.kill_server
    FileUtils.rm_f(@log_file)
  end

  it 'should have a users option array even with only a single user defined' do
    config_file = File.dirname(__FILE__) + '/resources/auth.yml'
    config = File.open(config_file) { |f| YAML.load(f) }
    NATSD::Server.process_options("-c #{config_file}".split)
    opts = NATSD::Server.options
    user = config['authorization']['user']
    pass = config['authorization']['pass']
    opts[:user].should == user
    opts[:pass].should == pass
    opts[:users].should_not be_nil
    opts[:users].should be_an_instance_of Array
    first = opts[:users].first
    first[:user].should == user
    first[:pass].should == pass
  end

  it 'should have a users array when user passed in on command line' do
    user = 'derek'
    pass = 'foo'
    NATSD::Server.process_options("--user #{user} --pass #{pass}".split)
    opts = NATSD::Server.options
    opts[:user].should == user
    opts[:pass].should == pass
    opts[:users].should_not be_nil
    opts[:users].should be_an_instance_of Array
    first = opts[:users].first
    first[:user].should == user
    first[:pass].should == pass
  end

  it 'should support mixed auth models and report singelton correctly' do
    config_file = File.dirname(__FILE__) + '/resources/mixed_auth.yml'
    config = File.open(config_file) { |f| YAML.load(f) }
    NATSD::Server.process_options("-c #{config_file}".split)
    opts = NATSD::Server.options
    user = config['authorization']['user']
    pass = config['authorization']['pass']
    opts[:user].should == user
    opts[:pass].should == pass
    opts[:users].should_not be_nil
    opts[:users].should be_an_instance_of Array
    first = opts[:users].first
    first[:user].should == user
    first[:pass].should == pass
  end

  it 'should accept pass or password in multi form' do
    config_file = File.dirname(__FILE__) + '/resources/multi_user_auth_long.yml'
    config = File.open(config_file) { |f| YAML.load(f) }
    NATSD::Server.process_options("-c #{config_file}".split)
    opts = NATSD::Server.options
    opts[:users].should_not be_nil
    opts[:users].should be_an_instance_of Array
    opts[:users].each { |u| u[:pass].should_not be_nil }
  end

  it 'should report first auth user as main user for backward compatability' do
    first = @opts[:users].first
    @opts[:user].should == first[:user]
    @opts[:pass].should == first[:pass]
  end

  it 'should not allow unauthorized access with multi auth' do
    expect do
      NATS.start(:uri => @uri) { NATS.stop }
    end.to raise_error NATS::Error
  end

  it 'should allow multi-users to be configured for auth' do
    @opts[:users].length.should >= 3

    user1 = @opts[:users][0][:user]
    pass1 = @opts[:users][0][:pass]

    user2 = @opts[:users][1][:user]
    pass2 = @opts[:users][1][:pass]

    user3 = @opts[:users][2][:user]
    pass3 = @opts[:users][2][:pass]

    expect do
      uri = "nats://#{user1}:#{pass1}@#{@host}:#{@port}"
      NATS.start(:uri => uri) { NATS.stop }
    end.to_not raise_error

    expect do
      uri = "nats://#{user2}:#{pass2}@#{@host}:#{@port}"
      NATS.start(:uri => uri) { NATS.stop }
    end.to_not raise_error

    expect do
      uri = "nats://#{user3}:#{pass3}@#{@host}:#{@port}"
      NATS.start(:uri => uri) { NATS.stop }
    end.to_not raise_error

  end

end
