source "http://rubygems.org"

gem 'eventmachine', '>= 0.12.10'
gem 'daemons', '>= 1.1.0'
gem 'yajl-ruby', '>= 0.7.8', :require => ['yajl', 'yajl/json_gem'], :platforms => [:mri_18, :mri_19]

platforms :jruby do
  gem 'json_pure', :require => 'json'
end

group :test do
  gem 'rspec'
  gem 'nats', :path => '.'
end
