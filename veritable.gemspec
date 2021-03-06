# -*- encoding: utf-8 -*-
require File.expand_path('../lib/veritable/version', __FILE__)

Gem::Specification.new do |gem|
  gem.authors       = ["Prior Knowledge"]
  gem.email         = ["support@priorknowledge.com"]
  gem.description   = "Veritable is the predictive database developed by Prior Knowledge (http://www.priorknowledge.com)"
  gem.summary       = "Ruby client for Veritable API"
  gem.homepage      = "https://dev.priorknowledge.com"

  gem.files         = Dir["**/*"].select { |d| d =~ %r{^(README.md|LICENSE|CHANGELOG.txt|lib/)} }
  gem.name          = "veritable"
  gem.require_paths = %w{lib}
  gem.version       = Veritable::VERSION
  
  gem.add_dependency('rest-client', '~> 1.4')
  gem.add_dependency('uuid')
  gem.add_dependency('multi_json')
  gem.add_dependency('backports')
  gem.add_development_dependency('test-unit')
  gem.add_development_dependency('rake')
  gem.add_development_dependency('rdoc')
  gem.add_development_dependency('simplecov')
  gem.add_development_dependency('json')  
end
