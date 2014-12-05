
module NATSD #:nodoc:

  VERSION  = '0.5.0.beta.16'
  APP_NAME = 'nats-server'

  DEFAULT_PORT = 4222
  DEFAULT_HOST = '0.0.0.0'

  # Parser
  AWAITING_CONTROL_LINE = 1
  AWAITING_MSG_PAYLOAD  = 2

  # Ops - See protocol.txt for more info
  INFO     = /\AINFO\s*([^\r\n]*)\r\n/i
  PUB_OP   = /\APUB\s+([^\s]+)\s+(([^\s]+)[^\S\r\n]+)?(\d+)\r\n/i
  MSG      = /\AMSG\s+([^\s]+)\s+([^\s]+)\s+(([^\s]+)[^\S\r\n]+)?(\d+)\r\n/i
  SUB_OP   = /\ASUB\s+([^\s]+)\s+(([^\s]+)[^\S\r\n]+)?([^\s]+)\r\n/i
  UNSUB_OP = /\AUNSUB\s+([^\s]+)\s*(\s+(\d+))?\r\n/i
  PING     = /\APING\s*\r\n/i
  PONG     = /\APONG\s*\r\n/i
  INFO_REQ = /\AINFO_REQ\s*\r\n/i

  CONNECT  = /\ACONNECT\s+([^\r\n]+)\r\n/i
  UNKNOWN  = /\A(.*)\r\n/
  CTRL_C   = /\006/
  CTRL_D   = /\004/

  ERR_RESP = /\A-ERR\s+('.+')?\r\n/i
  OK_RESP  = /\A\+OK\s*\r\n/i #:nodoc:

  # RESPONSES
  CR_LF = "\r\n".freeze
  CR_LF_SIZE = CR_LF.bytesize
  EMPTY = ''.freeze
  OK = "+OK#{CR_LF}".freeze
  PING_RESPONSE = "PING#{CR_LF}".freeze
  PONG_RESPONSE = "PONG#{CR_LF}".freeze
  INFO_RESPONSE = "#{CR_LF}".freeze

  # ERR responses
  PAYLOAD_TOO_BIG     = "-ERR 'Payload size exceeded'#{CR_LF}".freeze
  PROTOCOL_OP_TOO_BIG = "-ERR 'Protocol Operation size exceeded'#{CR_LF}".freeze
  INVALID_SUBJECT     = "-ERR 'Invalid Subject'#{CR_LF}".freeze
  INVALID_SID_TAKEN   = "-ERR 'Invalid Subject Identifier (sid), already taken'#{CR_LF}".freeze
  INVALID_SID_NOEXIST = "-ERR 'Invalid Subject-Identifier (sid), no subscriber registered'#{CR_LF}".freeze
  INVALID_CONFIG      = "-ERR 'Invalid config, valid JSON required for connection configuration'#{CR_LF}".freeze
  AUTH_REQUIRED       = "-ERR 'Authorization is required'#{CR_LF}".freeze
  AUTH_FAILED         = "-ERR 'Authorization failed'#{CR_LF}".freeze
  SSL_REQUIRED        = "-ERR 'TSL/SSL is required'#{CR_LF}".freeze
  SSL_FAILED          = "-ERR 'TLS/SSL failed'#{CR_LF}".freeze
  UNKNOWN_OP          = "-ERR 'Unknown Protocol Operation'#{CR_LF}".freeze
  SLOW_CONSUMER       = "-ERR 'Slow consumer detected, connection dropped'#{CR_LF}".freeze
  UNRESPONSIVE        = "-ERR 'Unresponsive client detected, connection dropped'#{CR_LF}".freeze
  MAX_CONNS_EXCEEDED  = "-ERR 'Maximum client connections exceeded, connection dropped'#{CR_LF}".freeze

  # Pedantic Mode
  SUB = /^([^\.\*>\s]+|>$|\*)(\.([^\.\*>\s]+|>$|\*))*$/
  SUB_NO_WC = /^([^\.\*>\s]+)(\.([^\.\*>\s]+))*$/

  # Router Subscription Identifiers
  RSID = /RSID:(\d+):(\S+)/

  # Some sane default thresholds

  # 1k should be plenty since payloads sans connect string are separate
  MAX_CONTROL_LINE_SIZE = 1024

  # Should be using something different if > 1MB payload
  MAX_PAYLOAD_SIZE = (1024*1024)

  # Maximum outbound size per client
  MAX_PENDING_SIZE = (10*1024*1024)

  # Maximum pending bucket size
  MAX_WRITEV_SIZE = (64*1024)

  # Maximum connections default
  DEFAULT_MAX_CONNECTIONS = (64*1024)

  # TLS/SSL wait time
  SSL_TIMEOUT = 0.5

  # Authorization wait time
  AUTH_TIMEOUT = SSL_TIMEOUT + 0.5

  # Ping intervals
  DEFAULT_PING_INTERVAL = 120
  DEFAULT_PING_MAX = 2

  # Route Reconnect
  DEFAULT_ROUTE_RECONNECT_INTERVAL = 1.0

  # HTTP
  RACK_JSON_HDR = { 'Content-Type' => 'application/json' }
  RACK_TEXT_HDR = { 'Content-Type' => 'text/plain' }

end
