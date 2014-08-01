desc "Run rspec"
task :spec do
  require "rspec/core/rake_task"
  RSpec::Core::RakeTask.new do |t|
    t.rspec_opts = %w(-fd -c)
  end
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
