begin
  require 'yajl'
  require 'yajl/json_gem'
rescue LoadError
  require 'json'
end
