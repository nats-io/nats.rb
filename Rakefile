#!/usr/bin/env rake
require "bundler/gem_tasks"

require 'rspec/core'
require 'rspec/core/rake_task'

RSpec::Core::RakeTask.new(:spec) do |spec|
  spec.pattern = FileList['spec/client/*_spec.rb']
  spec.rspec_opts = ["--format", "documentation", "--colour"]
end
task :default => :spec

desc "Build the gem"
task :gem do
  sh 'gem build *.gemspec'
end

desc "Install the gem"
task :geminstall do
  sh 'gem build *.gemspec'
  sh 'gem install *.gem'
  sh 'rm *.gem'
end

desc "Synonym for spec"
task :test => :spec
desc "Synonym for spec"
task :tests => :spec

desc "Synonym for gem"
task :pkg => :gem
desc "Synonym for gem"
task :package => :gem
