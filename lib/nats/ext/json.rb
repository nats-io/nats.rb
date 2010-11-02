begin
  require 'yajl'
  require 'yajl/json_gem'
rescue LoadError
  require 'rubygems'
  require 'json'
end
