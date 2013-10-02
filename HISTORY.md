# HISTORY

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
