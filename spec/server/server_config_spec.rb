require 'spec_helper'
require 'nats/server/server'
require "nats/server/sublist"
require "nats/server/options"
require "nats/server/const"
require "nats/server/util"
require 'logger'

describe "server configuration" do

  it 'should return default options with no command line arguments' do
    NATSD::Server.process_options
    opts = NATSD::Server.options

    opts.should be_an_instance_of Hash
    opts.should have_key :port
    opts.should have_key :addr
    opts.should have_key :max_control_line
    opts.should have_key :max_payload
    opts.should have_key :max_pending
    opts.should have_key :max_connections

    opts[:port].should == NATSD::DEFAULT_PORT
    opts[:addr].should == NATSD::DEFAULT_HOST
  end

  it 'should allow an override with command line arguments' do
    NATSD::Server.process_options('-a localhost -p 5222 --user derek --pass foo'.split)
    opts = NATSD::Server.options

    opts[:addr].should == 'localhost'
    opts[:port].should == 5222
    opts[:user].should == 'derek'
    opts[:pass].should == 'foo'
  end

  it 'should properly parse a config file' do
    config_file = File.dirname(__FILE__) + '/resources/config.yml'
    config = File.open(config_file) { |f| YAML.load(f) }
    NATSD::Server.process_options("-c #{config_file}".split)
    opts = NATSD::Server.options
    opts[:config_file].should == config_file
    opts[:port].should == config['port']
    opts[:addr].should == config['net']
    opts[:user].should == config['authorization']['user']
    opts[:pass].should == config['authorization']['password']
    opts[:token].should == config['authorization']['token']
    opts[:ssl].should == config['ssl']
    opts[:pid_file].should == config['pid_file']
    opts[:log_file].should == config['log_file']
    opts[:log_time].should == config['logtime']
    opts[:debug].should == config['debug']
    opts[:trace].should == config['trace']
    opts[:max_control_line].should == config['max_control_line']
    opts[:max_payload].should == config['max_payload']
    opts[:max_pending].should == config['max_pending']
    opts[:max_connections].should == config['max_connections']

  end

  it 'should allow pass and password for authorization config' do
    config_file = File.dirname(__FILE__) + '/resources/auth.yml'
    config = File.open(config_file) { |f| YAML.load(f) }
    NATSD::Server.process_options("-c #{config_file}".split)
    opts = NATSD::Server.options
    opts[:user].should == config['authorization']['user']
    opts[:pass].should == config['authorization']['pass']
  end

  it 'should allow command line arguments to override config file' do
    config_file = File.dirname(__FILE__) + '/resources/config.yml'
    config = File.open(config_file) { |f| YAML.load(f) }
    NATSD::Server.process_options("-c #{config_file} -p 8122 -l /tmp/foo.log".split)
    opts = NATSD::Server.options

    opts[:port].should == 8122
    opts[:log_file].should == '/tmp/foo.log'
  end

  it 'should properly set logtime under server attributes' do
    config_file = File.dirname(__FILE__) + '/resources/config.yml'
    config = File.open(config_file) { |f| YAML.load(f) }
    NATSD::Server.process_options("-c #{config_file}".split)
    NATSD::Server.finalize_options
    NATSD::Server.log_time.should be_truthy
  end

  describe "NATSD::Server.finalize_options" do
    before do
      NATSD::Server.process_options
    end

    context "ssl setting is nothing" do
      before do
        NATSD::Server.options[:ssl] = nil
      end

      it "shoud properly set @ssl_required to nil" do
        NATSD::Server.finalize_options
        NATSD::Server.ssl_required.should be_falsey
      end
    end

    context "ssl setting is false" do
      before do
        NATSD::Server.options[:ssl] = false
      end

      it "should properly set @ssl_required to false" do
        NATSD::Server.finalize_options
        NATSD::Server.ssl_required.should be_falsey
      end
    end

    context "ssl setting is true" do
      before do
        NATSD::Server.options[:ssl] = true
      end

      it "should properly set @ssl_required to true" do
        NATSD::Server.finalize_options
        NATSD::Server.ssl_required.should be_truthy
      end
    end
  end
end
