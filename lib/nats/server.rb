
require 'socket'
require 'fileutils'
require 'pp'
require 'syslog'

ep = File.expand_path(File.dirname(__FILE__))

require "#{ep}/ext/em"
require "#{ep}/ext/bytesize"
require "#{ep}/ext/json"
require "#{ep}/server/server"
require "#{ep}/server/sublist"
require "#{ep}/server/connection"
require "#{ep}/server/options"
require "#{ep}/server/cluster"
require "#{ep}/server/route"
require "#{ep}/server/const"
require "#{ep}/server/util"
require "#{ep}/server/varz"
require "#{ep}/server/connz"

# Do setup
NATSD::Server.setup(ARGV.dup)

# Event Loop
EM.run do

  log "Starting #{NATSD::APP_NAME} version #{NATSD::VERSION} on port #{NATSD::Server.port}"
  log "TLS/SSL Support Enabled" if NATSD::Server.options[:ssl]
  begin
    EM.set_descriptor_table_size(32768) # Requires Root privileges
    EM.start_server(NATSD::Server.host, NATSD::Server.port, NATSD::Connection)
  rescue => e
    log "Could not start server on port #{NATSD::Server.port}"
    log_error
    exit(1)
  end

  # Check to see if we need to fire up the http monitor port and server
  if NATSD::Server.options[:http_port]
    begin
      NATSD::Server.start_http_server
    rescue => e
      log "Could not start monitoring server on port #{NATSD::Server.options[:http_port]}"
      log_error
      exit(1)
    end
  end

  ###################
  # CLUSTER SETUP
  ###################

  # Check to see if we need to fire up a routing listen port
  if NATSD::Server.options[:cluster_port]
    begin
      log "Starting routing on port #{NATSD::Server.options[:cluster_port]}"
      EM.start_server(NATSD::Server.host, NATSD::Server.options[:cluster_port], NATSD::Route)
    rescue => e
      log "Could not start routing server on port #{NATSD::Server.options[:cluster_port]}"
      log_error
      exit(1)
    end
  end

  # If we have active connections, solicit them now..
  NATSD::Server.solicit_routes if NATSD::Server.options[:cluster_routes]

end
