require 'rubygems'
require 'openssl'

require 'veritable/api'
require 'veritable/connection'
require 'veritable/errors'
require 'veritable/util'
require 'veritable/version'

gem 'rest-client', '~> 1.4'
require 'rest_client'

gem "uuid", "~> 2.3.5"
require 'uuid'

gem "multi_json"
require 'multi_json'

module Veritable
  USER_AGENT = 'veritable-ruby ' + VERSION
  
  def self.connect(opts={})
    opts[:api_key] = opts[:api_key] || ENV['VERITABLE_KEY']
    opts[:api_url] = opts[:api_url] || ENV['VERITABLE_URL'] || "https://api.priorknowledge.com"

    opts[:ssl_verify] = true unless opts.has_key?(:ssl_verify)
    opts[:enable_gzip] = true unless opts.has_key?(:enable_gzip)

    api = API.new(opts)
    connection_test = api.root
    status = connection_test["status"]
    entropy = connection_test["entropy"]
    raise VeritableError.new("No Veritable server responding at #{opts[:api_url]}") if status != "SUCCESS"
    raise VeritableError.new("No Veritable server responding at #{opts[:api_url]}") if ! entropy.is_a?(Float)
    api
  end
end
