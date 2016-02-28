require 'spec_helper'
require 'nats/server/server'
require 'nats/server/sublist'
require 'nats/server/options'
require 'nats/server/const'
require 'nats/server/util'
require 'logger'

describe 'server cluster configuration' do

  it 'should allow the cluster listen port to be set on command line' do
    NATSD::Server.process_options('-a localhost -p 5222 -r 8222'.split)
    opts = NATSD::Server.options

    opts[:addr].should == 'localhost'
    opts[:port].should == 5222
    opts[:cluster_port].should == 8222
  end

  it 'should allow the cluster listen port to be set on command line (long form)' do
    NATSD::Server.process_options('-a localhost -p 5222 --cluster_port 8222'.split)
    opts = NATSD::Server.options

    opts[:addr].should == 'localhost'
    opts[:port].should == 5222
    opts[:cluster_port].should == 8222
  end

  it 'should properly parse a config file' do
    config_file = File.dirname(__FILE__) + '/resources/cluster.yml'
    config = File.open(config_file) { |f| YAML.load(f) }
    NATSD::Server.process_options("-c #{config_file}".split)
    opts = NATSD::Server.options
    opts[:config_file].should == config_file
    opts[:port].should == config['port']
    opts[:addr].should == config['net']
    opts[:user].should == config['authorization']['user']
    opts[:pass].should == config['authorization']['password']
    opts[:token].should == config['authorization']['token']
    opts[:pid_file].should == config['pid_file']
    opts[:log_file].should == config['log_file']
    opts[:log_time].should == config['logtime']
    opts[:debug].should == config['debug']
    opts[:trace].should == config['trace']
    opts[:max_control_line].should == config['max_control_line']
    opts[:max_payload].should == config['max_payload']
    opts[:max_pending].should == config['max_pending']

    # cluster specific
    opts[:cluster_port].should == config['cluster']['port']
    opts[:cluster_user].should == config['cluster']['authorization']['user']
    opts[:cluster_pass].should == config['cluster']['authorization']['password']
    opts[:cluster_token].should == config['cluster']['authorization']['token']
    opts[:cluster_auth_timeout].should == config['cluster']['authorization']['timeout']
    opts[:cluster_routes].should == config['cluster']['routes']
  end

end
