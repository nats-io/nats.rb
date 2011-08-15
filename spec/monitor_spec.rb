
require 'spec_helper'
require 'fileutils'

require 'net/http'
require 'uri'

require 'nats/server/server'
require 'nats/server/options'
require 'nats/server/const'
require 'nats/server/util'

describe 'monitor' do

  before (:all) do
    HTTP_SERVER_PID = '/tmp/nats_http.pid'
    HTTP_SERVER = 'nats://localhost:9229'
    LOG_FILE = '/tmp/nats_http.log'
    HTTP_PORT = 9230
    HTTP_FLAGS = "-m #{HTTP_PORT} -l #{LOG_FILE}"
    @s = NatsServerControl.new(HTTP_SERVER, HTTP_SERVER_PID, HTTP_FLAGS)
    @s.start_server
  end

  after(:all) do
    @s.kill_server
  end

  it 'should process simple command line arguments for http port' do
    NATSD::Server.process_options('-m 4222'.split)
    opts = NATSD::Server.options
    opts[:http_port].should == 4222
  end

  it 'should process long command line arguments for http port' do
    NATSD::Server.process_options('--http 4222'.split)
    opts = NATSD::Server.options
    opts[:http_port].should == 4222
  end

  it 'should properly parse http port from config file' do
    config_file = File.dirname(__FILE__) + '/resources/monitor.yml'
    config = File.open(config_file) { |f| YAML.load(f) }
    NATSD::Server.process_options("-c #{config_file}".split)
    opts = NATSD::Server.options
    opts[:http_port].should == 4222
    opts[:http_user].should == 'derek'
    opts[:http_password].should == 'foo'
  end

  it 'should start monitor http servers when requested' do
    begin
      sleep(0.5)
      s = TCPSocket.open(NATSD::Server.host, HTTP_PORT)
    ensure
      s.close if s
    end
  end

  it 'should return \'ok\' for /healthz' do
    host, port = NATSD::Server.host, HTTP_PORT
    healthz_req = Net::HTTP::Get.new("/healthz")
    healthz_resp = Net::HTTP.new(host, port).start { |http| http.request(healthz_req) }
    healthz_resp.body.should =~ /ok/i
  end

  it 'should return varz with proper members' do
    host, port = NATSD::Server.host, HTTP_PORT
    varz_req = Net::HTTP::Get.new("/varz")
    varz_resp = Net::HTTP.new(host, port).start { |http| http.request(varz_req) }
    varz_resp.body.should_not be_nil
    varz = JSON.parse(varz_resp.body, :symbolize_keys => true, :symbolize_names => true)
    varz.should be_an_instance_of Hash
    varz.should have_key :start
    varz.should have_key :options
    varz.should have_key :mem
    varz.should have_key :cpu
    varz.should have_key :cores
    varz.should have_key :connections
    varz.should have_key :in_msgs
    varz.should have_key :in_bytes
    varz.should have_key :out_msgs
    varz.should have_key :out_bytes

    # Check to make sure we pick up cores correctly
    varz[:cores].should > 0
  end

  it 'should properly track number of connections' do
    EM.run do
      (1..10).each { NATS.connect(:uri => HTTP_SERVER) }
      # Wait for them to register and varz to allow updates
      sleep(0.5)
      host, port = NATSD::Server.host, HTTP_PORT
      varz_req = Net::HTTP::Get.new("/varz")
      varz_resp = Net::HTTP.new(host, port).start { |http| http.request(varz_req) }
      varz_resp.body.should_not be_nil
      varz = JSON.parse(varz_resp.body, :symbolize_keys => true, :symbolize_names => true)
      varz[:connections].should == 10
      EM.stop
    end
  end

  it 'should track inbound and outbound message counts and bytes' do
    NATS.start(:uri => HTTP_SERVER) do
      NATS.subscribe('foo')
      NATS.subscribe('foo')
      (1..10).each {  NATS.publish('foo', 'hello world') }
      NATS.publish('foo') { NATS.stop }
    end
    sleep(0.5)
    host, port = NATSD::Server.host, HTTP_PORT
    varz_req = Net::HTTP::Get.new("/varz")
    varz_resp = Net::HTTP.new(host, port).start { |http| http.request(varz_req) }
    varz_resp.body.should_not be_nil
    varz = JSON.parse(varz_resp.body, :symbolize_keys => true, :symbolize_names => true)
    varz[:in_msgs].should == 11
    varz[:out_msgs].should == 22
    varz[:in_bytes].should == 110
    varz[:out_bytes].should == 220
  end

end
