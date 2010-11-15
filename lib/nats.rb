require 'uri'
require 'socket'
require 'fileutils'
require 'pp'
require 'optparse'
require 'yaml'
require 'nats/ext/json'
require 'nats/ext/em'
require 'nats/ext/bytesize'

module NATS
  VERSION = "0.3.11".freeze

  DEFAULT_PORT = 4222
  DEFAULT_URI = "nats://localhost:#{DEFAULT_PORT}".freeze

  CR_LF = "\r\n".freeze
  CR_LF_SIZE = CR_LF.bytesize

  PING_REQUEST  = "PING#{CR_LF}".freeze
  PONG_RESPONSE = "PONG#{CR_LF}".freeze

  EMPTY_MSG = ''.freeze

  MAX_RECONNECT_ATTEMPTS = 10
  RECONNECT_TIME_WAIT = 2 # in secs

  # Protocol
  MSG  = /^MSG\s+(\S+)\s+(\S+)\s+((\S+)\s+)?(\d+)$/i
  OK   = /^\+OK/i
  ERR  = /^-ERR\s+('.+')?/i
  PING = /^PING/i
  PONG = /^PONG/i
  INFO = /^INFO\s+(.+)/i

  # Pedantic Mode
  SUB = /^([^\.\*>\s]+|>$|\*)(\.([^\.\*>\s]+|>$|\*))*$/
  SUB_NO_WC = /^([^\.\*>\s]+)(\.([^\.\*>\s]+))*$/

  # Duplicate autostart protection
  @@tried_autostart = {}

  class Error < StandardError; end

  class << self
    attr_reader :client, :reactor_was_running, :err_cb, :err_cb_overridden
    alias :reactor_was_running? :reactor_was_running

    def connect(options = {}, &blk)
      options[:uri] ||= ENV['NATS_URI'] || DEFAULT_URI
      options[:debug] ||= ENV['NATS_DEBUG']
      options[:autostart] = (ENV['NATS_AUTO'] || true) unless options[:autostart] != nil
      uri = options[:uri] = URI.parse(options[:uri])
      @err_cb = proc { raise Error, "Could not connect to server on #{uri}."} unless err_cb
      check_autostart(uri) if options[:autostart]
      client = EM.connect(uri.host, uri.port, self, options)
      client.on_connect(&blk) if blk
      return client
    end

    def start(*args, &blk)
      @reactor_was_running = EM.reactor_running?
      unless (@reactor_was_running || blk)
        raise(Error, "EM needs to be running when NATS.start called without a run block")
      end
      EM.run { @client = connect(*args, &blk) }
    end

    def stop(&blk)
      client.close if (client and client.connected?)
      blk.call if blk
    end

    def on_error(&callback)
      @err_cb, @err_cb_overridden = callback, true
    end

    # Mirror instance methods for our client
    def publish(*args, &blk)
      (@client ||= connect).publish(*args, &blk)
    end

    def subscribe(*args, &blk)
      (@client ||= connect).subscribe(*args, &blk)
    end

    def unsubscribe(*args)
      (@client ||= connect).unsubscribe(*args)
    end

    def request(*args, &blk)
      (@client ||= connect).request(*args, &blk)
    end

    # utils
    def create_inbox
      v = [rand(0x0010000),rand(0x0010000),rand(0x0010000),
           rand(0x0010000),rand(0x0010000),rand(0x1000000)]
      "_INBOX.%04x%04x%04x%04x%04x%06x" % v
    end

    def check_autostart(uri)
      return if uri_is_remote?(uri) || @@tried_autostart[uri]
      @@tried_autostart[uri] = true
      return if server_running?(uri)
      return unless try_autostart_succeeded?(uri)
      wait_for_server(uri)
    end

    def uri_is_remote?(uri)
      uri.host != 'localhost' && uri.host != '127.0.0.1'
    end

    def try_autostart_succeeded?(uri)
      port_arg = "-p #{uri.port}"
      user_arg = "--user #{uri.user}" if uri.user
      pass_arg = "--pass #{uri.password}" if uri.password
      log_arg  = '-l /tmp/nats-server.log'
      pid_arg  = '-P /tmp/nats-server.pid'
      # daemon mode to release client
      system("nats-server #{port_arg} #{user_arg} #{pass_arg} #{log_arg} #{pid_arg} -d 2> /dev/null")
      $? == 0
    end

    def wait_for_server(uri)
      start = Time.now
      while (Time.now - start < 5) # Wait 5 seconds max
        break if server_running?(uri)
        sleep(0.1)
      end
    end

    def server_running?(uri)
      require 'socket'
      s = TCPSocket.new(uri.host, uri.port)
      s.close
      return true
    rescue
      return false
    end
  end
end

