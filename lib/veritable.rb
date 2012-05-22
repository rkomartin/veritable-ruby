require 'rubygems'
require 'openssl'
require 'json'

require 'veritable/api'
require 'veritable/connection'
require 'veritable/errors'
require 'veritable/util'
require 'veritable/version'

gem 'rest-client', '~> 1.4'
require 'rest_client'

module Veritable
  USER_AGENT = 'veritable-ruby ' + VERSION
  
  def self.connect(api_key=nil, api_url=nil, opts={})
    opts[:api_key] = api_key || opts[:api_key] || ENV['VERITABLE_KEY']
    opts[:api_url] = api_url || opts[:api_url] || ENV['VERITABLE_URL'] || "https://api.priorknowledge.com"

    opts[:ssl_verify] = true unless opts.has_key?(:ssl_verify)
    opts[:enable_gzip] = true unless opts.has_key?(:enable_gzip)

    api = API.new(opts)
    connection_test = api.root
    status = connection_test["status"]
    entropy = connection_test["entropy"]
    raise VeritableError if status != "SUCCESS"
    raise VeritableError if ! entropy.is_a?(Float)
    api
  end
end
