module NATS
  
  VERSION = "0.2.1"
    
  # Ops
  INFO = /^INFO$/i
  PUB_OP = /^PUB\s+(\S+)\s+((\S+)\s+)?(\d+)$/i
  SUB_OP = /^SUB\s+(\S+)\s+(\S+)$/i
  UNSUB_OP = /^UNSUB\s+(\S+)$/i  
  PING = /^PING$/i
  CONNECT = /^CONNECT\s+(.+)$/i
  
  # Should be using something different if > 1MB payload
  MAX_PAYLOAD_SIZE = 1024 * 1024

  # RESPONSES
  CR_LF = "\r\n".freeze
  CR_LF_SIZE = CR_LF.bytesize
  OK = "+OK #{CR_LF}".freeze  
  PONG_RESPONSE = "PONG#{CR_LF}".freeze

  INFO_RESPONSE = "#{CR_LF}".freeze

  # ERR responses
  PAYLOAD_TOO_BIG = "-ERR 'Payload size exceeded, max is #{MAX_PAYLOAD_SIZE} bytes'#{CR_LF}".freeze
  INVALID_SUBJECT = "-ERR 'Invalid Subject'#{CR_LF}".freeze
  INVALID_SID_TAKEN = "-ERR 'Invalid Subject Identifier (sid), already taken'#{CR_LF}".freeze
  INVALID_SID_NOEXIST = "-ERR 'Invalid Subject-Identifier (sid), no subscriber registered'#{CR_LF}".freeze
  INVALID_CONFIG = "-ERR 'Invalid config, valid JSON required for connection configuration'#{CR_LF}".freeze
  AUTH_REQUIRED = "-ERR 'Authorization is required'#{CR_LF}".freeze
  AUTH_FAILED = "-ERR 'Authorization failed'#{CR_LF}".freeze

  # Pedantic Mode
  SUB = /^([^\.\*>\s]+|>$|\*)(\.([^\.\*>\s]+|>$|\*))*$/
  SUB_NO_WC = /^([^\.\*>\s]+)(\.([^\.\*>\s]+))*$/

  # Autorization wait time
  AUTH_TIMEOUT = 5

end
