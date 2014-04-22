require 'spec_helper'
require 'fileutils'

describe 'ssl' do

  before (:all) do
    TEST_SERVER_SSL = "nats://localhost:9392"
    TEST_SERVER_SSL_PID = '/tmp/nats_ssl.pid'

    TEST_SERVER_NO_SSL = "nats://localhost:9394"
    TEST_SERVER_NO_SSL_PID = '/tmp/nats_no_ssl.pid'

    @s_ssl = NatsServerControl.new(TEST_SERVER_SSL, TEST_SERVER_SSL_PID, "--ssl")
    @s_ssl.start_server

    @s_no_ssl = NatsServerControl.new(TEST_SERVER_NO_SSL, TEST_SERVER_NO_SSL_PID)
    @s_no_ssl.start_server
  end

  after (:all) do
    @s_ssl.kill_server
    @s_no_ssl.kill_server
    FileUtils.rm_f TEST_SERVER_SSL_PID
    FileUtils.rm_f TEST_SERVER_NO_SSL_PID
  end

  it 'should fail to connect to an ssl server without TLS/SSL negotiation' do
    expect do
      NATS.start(:uri => TEST_SERVER_SSL) { NATS.stop }
    end.to raise_error NATS::Error
  end

  it 'should fail to connect to an no ssl server with TLS/SSL negotiation' do
    expect do
      NATS.start(:uri => TEST_SERVER_NO_SSL, :ssl => true) { NATS.stop }
    end.to raise_error NATS::Error
  end

  it 'should run TLS/SSL negotiation' do
    expect do
      NATS.start(:uri => TEST_SERVER_SSL, :ssl => true) { NATS.stop }
    end.to_not raise_error
  end

  it 'should not run TLS/SSL negotiation' do
    expect do
      NATS.start(:uri => TEST_SERVER_NO_SSL) { NATS.stop }
    end.to_not raise_error
  end

end
