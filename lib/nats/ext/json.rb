begin
  require 'yajl'
  require 'yajl/json_gem'
rescue LoadError
  begin
    require 'oj'
    Oj.mimic_JSON()
  rescue LoadError
    require 'rubygems'
    require 'json'
  end
end
