
def fast_uuid #:nodoc:
  v = [rand(0x0010000),rand(0x0010000),rand(0x0010000),
       rand(0x0010000),rand(0x0010000),rand(0x1000000)]
  "%04x%04x%04x%04x%04x%06x" % v
end

def log(*args) #:nodoc:
  args.unshift(Time.now) if NATSD::Server.log_time
  pp args.compact
end

def debug(*args) #:nodoc:
  log *args if NATSD::Server.debug_flag?
end

def trace(*args) #:nodoc:
  log *args if NATSD::Server.trace_flag?
end

def log_error(e=$!) #:nodoc:
  debug e, e.backtrace
end

def shutdown #:nodoc:
  puts
  log 'Server exiting..'
  EM.stop
  FileUtils.rm(NATSD::Server.pid_file) if NATSD::Server.pid_file
  exit
end

['TERM','INT'].each { |s| trap(s) { shutdown } }

