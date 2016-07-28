#!/usr/bin/env rake
require 'rspec/core'
require 'rspec/core/rake_task'

task :default => 'spec:client'

desc 'Run specs from client and server'
RSpec::Core::RakeTask.new(:spec) do |spec|
  spec.pattern = FileList['spec/**/*_spec.rb']
  spec.rspec_opts = ["--format", "documentation", "--colour"]
end

desc 'Run spec from client using gnatsd as the server'
RSpec::Core::RakeTask.new('spec:client') do |spec|
  spec.pattern = FileList['spec/client/*_spec.rb']
  spec.rspec_opts = ["--format", "documentation", "--colour"]
end

desc 'Run spec from server'
RSpec::Core::RakeTask.new('spec:server') do |spec|
  spec.pattern = FileList['spec/server/*_spec.rb']
  spec.rspec_opts = ["--format", "documentation", "--colour"]
end

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
