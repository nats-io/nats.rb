
def fast_uuid #:nodoc:
  v = [rand(0x0010000),rand(0x0010000),rand(0x0010000),
       rand(0x0010000),rand(0x0010000),rand(0x1000000)]
  "%04x%04x%04x%04x%04x%06x" % v
end

def syslog(args, priority) #:nodoc:
  Syslog::log(priority, '%s', PP::pp(args.compact, '', 120))
end

def log(*args) #:nodoc:
  return syslog(args, Syslog::LOG_NOTICE) if NATSD::Server.syslog
  args.unshift(Time.now) if NATSD::Server.log_time
  PP::pp(args.compact, $stdout, 120)
end

def debug(*args) #:nodoc:
  return unless NATSD::Server.debug_flag?
  return syslog(args, Syslog::LOG_INFO) if NATSD::Server.syslog
  log(*args)
end

def trace(*args) #:nodoc:
  return unless NATSD::Server.trace_flag?
  return syslog(args, Syslog::LOG_DEBUG) if NATSD::Server.syslog
  log(*args)
end

def log_error(e=$!) #:nodoc:
  debug e, e.backtrace
end

def uptime_string(delta)
  num_seconds = delta.to_i
  days = num_seconds / (60 * 60 * 24);
  num_seconds -= days * (60 * 60 * 24);
  hours = num_seconds / (60 * 60);
  num_seconds -= hours * (60 * 60);
  minutes = num_seconds / 60;
  num_seconds -= minutes * 60;
  "#{days}d:#{hours}h:#{minutes}m:#{num_seconds}s"
end

def pretty_size(size, prec=1)
  return 'NA' unless size
  return "#{size}B" if size < 1024
  return sprintf("%.#{prec}fK", size/1024.0) if size < (1024*1024)
  return sprintf("%.#{prec}fM", size/(1024.0*1024.0)) if size < (1024*1024*1024)
  return sprintf("%.#{prec}fG", size/(1024.0*1024.0*1024.0))
end

def num_cpu_cores
  if RUBY_PLATFORM =~ /linux/
    return `cat /proc/cpuinfo | grep processor | wc -l`.to_i
  elsif RUBY_PLATFORM =~ /darwin/
    `sysctl -n hw.ncpu`.strip.to_i
  elsif RUBY_PLATFORM =~ /freebsd|netbsd/
    `sysctl hw.ncpu`.strip.to_i
  else
    return 1
  end
end

def shutdown #:nodoc:
  puts
  log 'Server exiting..'
  NATSD::Server.close_syslog
  EM.stop
  if NATSD::Server.pid_file
    FileUtils.rm(NATSD::Server.pid_file) if File.exists? NATSD::Server.pid_file
  end
  exit
end

['TERM','INT'].each { |s| trap(s) { shutdown } }

# FIXME - Should probably be smarter when lots of connections
def dump_connection_state
  log "Dumping connection state on SIG_USR2"
  ObjectSpace.each_object(NATSD::Connection) do |c|
    log c.info unless c.closing?
  end
  log 'Connection Dump Complete'
end

trap('USR2') { dump_connection_state }
