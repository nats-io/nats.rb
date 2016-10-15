#!/usr/bin/env rake
require 'rspec/core'
require 'rspec/core/rake_task'

desc 'Run specs from client'

RSpec::Core::RakeTask.new(:spec) do |spec|
  spec.pattern = FileList['spec/**/*_spec.rb']
  spec.rspec_opts = ["--format", "documentation", "--colour"]
end

task :default => :spec
