# HISTORY

## v0.8.2 (March 14, 2017)
  - Allow setting name from client on connect (#129)
  - Add discovered servers helper for servers announced via async INFO (#136)
  - Add time based reconnect backoff (#139)
  - Modify lang sent on connect when using jruby (#135)
  - Update eventmachine dependencies (#134)

## v0.8.0 (August 10, 2016)
  - Added cluster auto discovery handling which is supported on v0.9.2 server release (#125)
  - Added jruby part of the build (both in openjdk and oraclejdk runtimes) (#122 #123)
  - Fixed ping interval accounting (#120)

## v0.7.1 (July 8, 2016)
  - Remove dependencies which are no longer needed for ruby-client
  - See full list @ https://github.com/nats-io/ruby-nats/compare/v0.7.0...v0.7.1

## v0.7.0 (July 8, 2016)
  - Enhanced TLS support: certificates and verify peer functionality added
  - Bumped version of Eventmachine to 1.2 series
  - See full list @ https://github.com/nats-io/ruby-nats/compare/v0.6.0...v0.7.0

## v0.6.0 (March 22, 2016)
  - Removed distributing `nats-server` along with the gem
  - Fixed issue with subscriptions not being sent on first reconnect (#94)
  - Added loading Oj gem for JSON when supported (#91)
  - Fixed removing warning message introduced by EM 1.0.8 (#90)
  - Changed to testing spec with `gnatsd` (#95)
  - See full list @ https://github.com/nats-io/ruby-nats/compare/v0.5.1...v0.6.0

## v0.5.1 (August 7, 2015)
  - Changed to never remove servers when configured as such (#88)
  - See full list @ https://github.com/nats-io/ruby-nats/compare/v0.5.0...v0.5.1

## v0.5.0 (June 19, 2015)
  - See full list @ https://github.com/nats-io/ruby-nats/compare/v0.5.0.beta.16...v0.5.0

## v0.5.0.beta.16 (December 7, 2014)
  - Resolved major issue on cluster connects to non-first server, issue #78
  - Official Support for Ruby 2.1
  - See full list @ https://github.com/derekcollison/nats/compare/v0.5.0.beta.12...v0.5.0.beta.16

## v0.5.0.beta.12 (October 1, 2013)
  - Fixed issue #58, reconnects not stopped on auth failures
  - Fixed leaking ping timers on auth failures
  - Created AuthError
  - See full list @ https://github.com/derekcollison/nats/compare/v0.5.0.beta.11...v0.5.0.beta.12

## v0.5.0.beta.11 (July 26, 2013)
  - Bi-directional Route designation
  - Upgrade to EM 1.x
  - See full list @ https://github.com/derekcollison/nats/compare/v0.5.0.beta.1...v0.5.0.beta.11

## v0.5.0.beta.1 (Sept 10, 2012)
  - Clustering support for nats-servers
  - Reconnect client logic cluster aware (explicit servers only for now)
  - See full list @ https://github.com/derekcollison/nats/compare/v0.4.26...v0.5.0.beta.1

## v0.4.28 (September 22, 2012)
  - Binary payload bug fix
  - Lock EM to version 0.12.10, 1.0 does not pass tests currently.
  - See full list @ https://github.com/derekcollison/nats/compare/v0.4.26...v0.4.28

## v0.4.26 (July 30, 2012)
  - Syslog support
  - Fixed reconnect bug to authorized servers
  - See full list @ https://github.com/derekcollison/nats/compare/v0.4.24...v0.4.26

## v0.4.24 (May 24, 2012)

  - Persist queue groups across reconnects
  - Proper exit codes for nats-server
  - See full list @ https://github.com/derekcollison/nats/compare/v0.4.22...v0.4.24

## v0.4.22 (Mar 5, 2012)

  - HTTP based server monitoring (/varz, /connz, /healthz)
  - Perfomance and Stability improvements
  - Client monitoring
  - Server to Client pings
  - Multiple Auth users
  - SSL/TSL support
  - nats-top utility
  - Connection state dump on SIGUSR2
  - Client Server information support
  - Client Fast Producer support
  - Client reconenct callbacks
  - Server Max Connections support
  - See full list @ https://github.com/derekcollison/nats/compare/v0.4.10...v0.4.22

## v0.4.10 (Apr 21, 2011)

  - Minor bug fixes
  - See full list @ https://github.com/derekcollison/nats/compare/v0.4.8...v0.4.10

## v0.4.8 (Apr 2, 2011)

  - Minor bug fixes
  - See full list @ https://github.com/derekcollison/nats/compare/v0.4.2...v0.4.8

## v0.4.2 (Feb 21, 2011)

  - Queue group support
  - Auto-unsubscribe support
  - Time expiration on subscriptions
  - Jruby initial support
  - Performance enhancements
  - Complete config file support
  - See full list @ https://github.com/derekcollison/nats/compare/v0.3.12...v0.4.2

## v0.3.12 (Nov 21, 2010)

  - Initial Release
