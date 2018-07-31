#!/usr/bin/env rake

require 'rspec/core'
require 'rspec/core/rake_task'

desc 'Run specs from client'

RSpec::Core::RakeTask.new(:spec) do |spec|
  spec.pattern = FileList['spec/**/*_spec.rb']
  opts = ["--format", "documentation", "--colour"]

  # Skip TLS hostname verification tests since not supported
  # until after Ruby 2.4.0.  Also skip in JRuby since looks like support pending.
  major_version, minor_version, _ = RUBY_VERSION.split('.').map(&:to_i)
  if major_version < 2 or (major_version >= 2 and minor_version < 4) or RUBY_PLATFORM == "java"
    opts << ["--tag", "~tls_verify_hostname"]
  end
  spec.rspec_opts = opts.flatten
end

task :default => :spec
