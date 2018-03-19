#!/usr/bin/env rake

require 'rspec/core'
require 'rspec/core/rake_task'

desc 'Run specs from client'

RSpec::Core::RakeTask.new(:spec) do |spec|
  spec.pattern = FileList['spec/**/*_spec.rb']
  opts = ["--format", "documentation", "--colour"]

  # Skip TLS hostname verification tests since not supported
  # until after Ruby 2.4.0.
  major_version, minor_version, _ = RUBY_VERSION.split('.').map(&:to_i)
  if major_version < 2 or (major_version >= 2 and minor_version < 4)
    opts << ["--tag", "~tls_verify_hostname"]
  end
  spec.rspec_opts = opts.flatten
end

task :default => :spec
