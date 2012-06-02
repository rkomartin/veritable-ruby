require 'openssl'

require 'veritable/api'
require 'veritable/connection'
require 'veritable/errors'
require 'veritable/util'
require 'veritable/version'

require 'rest_client'
require 'uuid'
require 'multi_json'

module Veritable
  USER_AGENT = 'veritable-ruby ' + VERSION
  BASE_URL = "https://api.priorknowledge.com"
  
  def self.connect(opts={})
    opts[:api_key] = opts[:api_key] || ENV['VERITABLE_KEY']
    opts[:api_base_url] = opts[:api_base_url] || ENV['VERITABLE_URL'] || BASE_URL

    opts[:ssl_verify] = true unless opts.has_key?(:ssl_verify)
    opts[:enable_gzip] = true unless opts.has_key?(:enable_gzip)

    api = API.new(opts)
    connection_test = api.root
    status = connection_test["status"]
    entropy = connection_test["entropy"]
    raise VeritableError.new("No Veritable server responding at #{opts[:api_base_url]}") if status != "SUCCESS"
    raise VeritableError.new("No Veritable server responding at #{opts[:api_base_url]}") if ! entropy.is_a?(Float)
    api
  end
end
