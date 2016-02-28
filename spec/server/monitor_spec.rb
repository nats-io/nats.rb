
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
    FileUtils.rm_f(LOG_FILE)
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
    opts[:http_net].should == '127.0.0.1'
    opts[:http_port].should == 4222
    opts[:http_user].should == 'derek'
    opts[:http_password].should == 'foo'
  end

  it 'should start monitor http servers when requested' do
    total_wait, now = 0, Time.now
    begin
      s = TCPSocket.open(NATSD::Server.host, HTTP_PORT)
    rescue Errno::ECONNREFUSED => e
      total_wait = Time.now - now
      now = Time.now
      retry unless total_wait > 5
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
    varz.should have_key :routes
    varz.should have_key :in_msgs
    varz.should have_key :in_bytes
    varz.should have_key :out_msgs
    varz.should have_key :out_bytes

    # Check to make sure we pick up cores correctly
    varz[:cores].should > 0
  end

  it 'should properly track number of connections' do
    EM.run do
      conns = []
      (1..10).each { conns << NATS.connect(:uri => HTTP_SERVER) }
      # Wait for them to register and varz to allow updates
      wait_on_connections(conns) do
        host, port = NATSD::Server.host, HTTP_PORT
        varz_req = Net::HTTP::Get.new("/varz")
        varz_resp = Net::HTTP.new(host, port).start { |http| http.request(varz_req) }
        varz_resp.body.should_not be_nil
        varz = JSON.parse(varz_resp.body, :symbolize_keys => true, :symbolize_names => true)
        varz[:connections].should == 10

        conns.each { |c| c.close }
        EM.stop
      end
    end
  end

  it 'should track inbound and outbound message counts and bytes' do
    NATS.start(:uri => HTTP_SERVER) do
      NATS.subscribe('foo')
      NATS.subscribe('foo')
      (1..11).each {  NATS.publish('foo', 'hello world') }
      NATS.flush do
        host, port = NATSD::Server.host, HTTP_PORT
        varz_req = Net::HTTP::Get.new("/varz")
        varz_resp = Net::HTTP.new(host, port).start { |http| http.request(varz_req) }
        varz_resp.body.should_not be_nil
        varz = JSON.parse(varz_resp.body, :symbolize_keys => true, :symbolize_names => true)
        varz[:in_msgs].should == 11
        varz[:out_msgs].should == 22
        varz[:in_bytes].should == 121
        varz[:out_bytes].should == 242
        NATS.stop
      end
    end
  end

  it 'should return connz with proper members' do
    EM.run do
      conns = []
      (1..10).each { conns << NATS.connect(:uri => HTTP_SERVER) }
      # Wait for them to register and connz to allow updates
      wait_on_connections(conns) do
        host, port = NATSD::Server.host, HTTP_PORT
        connz_req = Net::HTTP::Get.new("/connz")
        connz_resp = Net::HTTP.new(host, port).start { |http| http.request(connz_req) }
        connz_resp.body.should_not be_nil
        connz = JSON.parse(connz_resp.body, :symbolize_keys => true, :symbolize_names => true)
        connz.should have_key :pending_size
        connz.should have_key :num_connections
        connz[:num_connections].should == 10
        connz[:connections].size.should == 10
        c_info = connz[:connections].first
        c_info.should have_key :cid
        c_info.should have_key :ip
        c_info.should have_key :port
        c_info.should have_key :subscriptions
        c_info.should have_key :pending_size
        c_info.should have_key :in_msgs
        c_info.should have_key :out_msgs
        c_info.should have_key :in_bytes
        c_info.should have_key :out_bytes

        conns.each { |c| c.close }
        EM.stop
      end
    end
  end

  it 'should return connz with subset of connections if requested' do
    EM.run do
      conns = []
      (1..50).each { conns << NATS.connect(:uri => HTTP_SERVER) }
      # Wait for them to register and connz to allow updates
      wait_on_connections(conns) do
        host, port = NATSD::Server.host, HTTP_PORT
        connz_req = Net::HTTP::Get.new("/connz?n=11")
        connz_resp = Net::HTTP.new(host, port).start { |http| http.request(connz_req) }
        connz_resp.body.should_not be_nil
        connz = JSON.parse(connz_resp.body, :symbolize_keys => true, :symbolize_names => true)
        connz.should have_key :pending_size
        connz.should have_key :num_connections
        connz[:num_connections].should == 50
        connz[:connections].size.should == 11
        c_info = connz[:connections].first
        c_info.should have_key :cid
        c_info.should have_key :ip
        c_info.should have_key :port
        c_info.should have_key :subscriptions
        c_info.should have_key :pending_size
        c_info.should have_key :in_msgs
        c_info.should have_key :out_msgs
        c_info.should have_key :in_bytes
        c_info.should have_key :out_bytes

        conns.each { |c| c.close }
        EM.stop
      end
    end
  end

  it 'should return connz with subset of connections sorted correctly if requested' do
    EM.run do
      conns = []

      (1..10).each do
        conns << NATS.connect(:uri => HTTP_SERVER)
      end
      (1..4).each do
        conns << c = NATS.connect(:uri => HTTP_SERVER)
        c.subscribe('foo')
        c.subscribe('foo')
      end

      conns << c = NATS.connect(:uri => HTTP_SERVER)
      (1..10).each { c.publish('foo', "hello world") }

      # Wait for them to register and connz to allow updates
      wait_on_connections(conns) do
        host, port = NATSD::Server.host, HTTP_PORT

        # Test different sorts

        # out_msgs
        connz_req = Net::HTTP::Get.new("/connz?n=4&s=out_msgs")
        connz_resp = Net::HTTP.new(host, port).start { |http| http.request(connz_req) }
        connz_resp.body.should_not be_nil

        connz = JSON.parse(connz_resp.body, :symbolize_keys => true, :symbolize_names => true)
        connz.should have_key :pending_size
        connz.should have_key :num_connections
        connz[:num_connections].should == 15
        connz[:connections].size.should == 4
        connz[:connections].each do |c_info|
          c_info[:out_msgs].should == 20
        end

        # msgs_to
        connz_req = Net::HTTP::Get.new("/connz?n=4&s=msgs_to")
        connz_resp = Net::HTTP.new(host, port).start { |http| http.request(connz_req) }
        connz_resp.body.should_not be_nil
        connz = JSON.parse(connz_resp.body, :symbolize_keys => true, :symbolize_names => true)
        connz.should have_key :pending_size
        connz.should have_key :num_connections
        connz[:num_connections].should == 15
        connz[:connections].size.should == 4
        connz[:connections].each do |c_info|
          c_info[:out_msgs].should == 20
        end

        # out_bytes
        connz_req = Net::HTTP::Get.new("/connz?n=2&s=out_bytes")
        connz_resp = Net::HTTP.new(host, port).start { |http| http.request(connz_req) }
        connz_resp.body.should_not be_nil
        connz = JSON.parse(connz_resp.body, :symbolize_keys => true, :symbolize_names => true)
        connz.should have_key :pending_size
        connz.should have_key :num_connections
        connz[:num_connections].should == 15
        connz[:connections].size.should == 2
        connz[:connections].each do |c_info|
          c_info[:out_bytes].should == 220
        end

        # bytes_to
        connz_req = Net::HTTP::Get.new("/connz?n=2&s=bytes_to")
        connz_resp = Net::HTTP.new(host, port).start { |http| http.request(connz_req) }
        connz_resp.body.should_not be_nil
        connz = JSON.parse(connz_resp.body, :symbolize_keys => true, :symbolize_names => true)
        connz.should have_key :pending_size
        connz.should have_key :num_connections
        connz[:num_connections].should == 15
        connz[:connections].size.should == 2
        connz[:connections].each do |c_info|
          c_info[:out_bytes].should == 220
        end

        # in_msgs
        connz_req = Net::HTTP::Get.new("/connz?n=1&s=in_msgs")
        connz_resp = Net::HTTP.new(host, port).start { |http| http.request(connz_req) }
        connz_resp.body.should_not be_nil
        connz = JSON.parse(connz_resp.body, :symbolize_keys => true, :symbolize_names => true)
        connz.should have_key :pending_size
        connz.should have_key :num_connections
        connz[:num_connections].should == 15
        connz[:connections].size.should == 1
        c_info = connz[:connections].first
        c_info[:in_msgs].should == 10


        # msgs_from
        connz_req = Net::HTTP::Get.new("/connz?n=1&s=msgs_from")
        connz_resp = Net::HTTP.new(host, port).start { |http| http.request(connz_req) }
        connz_resp.body.should_not be_nil
        connz = JSON.parse(connz_resp.body, :symbolize_keys => true, :symbolize_names => true)
        connz.should have_key :pending_size
        connz.should have_key :num_connections
        connz[:num_connections].should == 15
        connz[:connections].size.should == 1
        c_info = connz[:connections].first
        c_info[:in_msgs].should == 10

        # in_bytes
        connz_req = Net::HTTP::Get.new("/connz?n=1&s=in_bytes")
        connz_resp = Net::HTTP.new(host, port).start { |http| http.request(connz_req) }
        connz_resp.body.should_not be_nil
        connz = JSON.parse(connz_resp.body, :symbolize_keys => true, :symbolize_names => true)
        connz.should have_key :pending_size
        connz.should have_key :num_connections
        connz[:num_connections].should == 15
        connz[:connections].size.should == 1
        c_info = connz[:connections].first
        c_info[:in_bytes].should == 110

        # bytes_from
        connz_req = Net::HTTP::Get.new("/connz?n=1&s=bytes_from")
        connz_resp = Net::HTTP.new(host, port).start { |http| http.request(connz_req) }
        connz_resp.body.should_not be_nil
        connz = JSON.parse(connz_resp.body, :symbolize_keys => true, :symbolize_names => true)
        connz.should have_key :pending_size
        connz.should have_key :num_connections
        connz[:num_connections].should == 15
        connz[:connections].size.should == 1
        c_info = connz[:connections].first
        c_info[:in_bytes].should == 110

        # subscriptions (short form)
        connz_req = Net::HTTP::Get.new("/connz?n=1&s=subs")
        connz_resp = Net::HTTP.new(host, port).start { |http| http.request(connz_req) }
        connz_resp.body.should_not be_nil
        connz = JSON.parse(connz_resp.body, :symbolize_keys => true, :symbolize_names => true)
        connz.should have_key :pending_size
        connz.should have_key :num_connections
        connz[:num_connections].should == 15
        connz[:connections].size.should == 1
        c_info = connz[:connections].first
        c_info[:subscriptions].should == 2

        # subscriptions (long form)
        connz_req = Net::HTTP::Get.new("/connz?n=1&s=subscriptions")
        connz_resp = Net::HTTP.new(host, port).start { |http| http.request(connz_req) }
        connz_resp.body.should_not be_nil
        connz = JSON.parse(connz_resp.body, :symbolize_keys => true, :symbolize_names => true)
        connz.should have_key :pending_size
        connz.should have_key :num_connections
        connz[:num_connections].should == 15
        connz[:connections].size.should == 1
        c_info = connz[:connections].first
        c_info[:subscriptions].should == 2

        conns.each { |c| c.close }
        EM.stop
      end

    end
  end

  it 'should require auth if configured to do so' do
    config_file = File.dirname(__FILE__) + '/resources/monitor.yml'
    config = File.open(config_file) { |f| YAML.load(f) }
    uri = "nats://#{config['net']}:#{config['port']}"

    auth_s = NatsServerControl.new(uri, config['pid_file'], "-c #{config_file}")
    auth_s.start_server

    host, port = config['http']['net'], config['http']['port']

    begin
      sleep(0.5)
      s = TCPSocket.open(host, port)
    ensure
      s.close if s
    end

    varz_req = Net::HTTP::Get.new("/varz")
    varz_resp = Net::HTTP.new(host, port).start { |http| http.request(varz_req) }
    varz_resp.code.should_not == '200'
    varz_resp.body.should be_empty

    # Do proper auth here
    varz_req.basic_auth(config['http']['user'], config['http']['password'])
    varz_resp = Net::HTTP.new(host, port).start { |http| http.request(varz_req) }
    varz_resp.code.should == '200'
    varz_resp.body.should_not be_empty

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

    auth_s.kill_server if auth_s
  end

end
