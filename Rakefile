#!/usr/bin/env rake
# Copyright 2010-2018 The NATS Authors
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

require 'rspec/core'
require 'rspec/core/rake_task'

task :default => 'spec:client'

desc 'Run specs from client and server'
RSpec::Core::RakeTask.new(:spec) do |spec|
  spec.pattern = FileList['spec/**/*_spec.rb']
  spec.rspec_opts = ["--format", "documentation", "--colour", "--profile"]
end

desc 'Run spec from client using gnatsd as the server'
RSpec::Core::RakeTask.new('spec:client') do |spec|
  spec.pattern = FileList['spec/client/*_spec.rb']
  spec.rspec_opts = ["--format", "documentation", "--colour", "--profile"]
end

desc 'Run spec from client on jruby using gnatsd as the server'
RSpec::Core::RakeTask.new('spec:client:jruby') do |spec|
  spec.pattern = FileList['spec/client/*_spec.rb']
  spec.rspec_opts = ["--format", "documentation", "--colour", "--tag", "~jruby_excluded", "--profile"]
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
