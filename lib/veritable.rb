require 'rubygems'
require 'openssl'
require 'json'

require 'veritable/api'
require 'veritable/connection'
require 'veritable/version'

gem 'rest-client', '~> 1.4'
require 'rest_client'

module Veritable
  USER_AGENT = 'veritable-ruby ' + VERSION
  
  def self.connect(opts={})
    opts[:api_key] = opts[:api_key] || ENV['VERITABLE_KEY']
    opts[:api_url] = opts[:api_url] || ENV['VERITABLE_URL'] || "https://api.priorknowledge.com"

    opts[:ssl_verify] = true unless opts.has_key? :ssl_verify 
    opts[:enable_gzip] = true unless opts.has_key? :enable_gzip

    api = API.new(connection=opts)
    r = api.root
    api
  end
end
