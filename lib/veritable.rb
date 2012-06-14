require 'openssl'

require 'veritable/api'
require 'veritable/connection'
require 'veritable/errors'
require 'veritable/util'
require 'veritable/version'

require 'rest_client'
require 'uuid'
require 'multi_json'

# The main module for the Veritable client
#
# ==== Module methods
# Veritable.connect is the main entry point.
#
# ==== Classes
# Veritable::API represents a single user's tables and settings.
#
# Veritable::Table, Veritable::Analysis, and Veritable::Prediction represent API resources.
#
# Veritable::Schema represents schemas for Veritable analyses.
#
# Collections of API resources are returned as instances of Veritable::Cursor, an Enumerable.
#
# All errors are instances of Veritable::VeritableError.
#
# ==== Modules
# The Veritable::Connection module encapsulates the HTTP logic.
#
# The Veritable::Util module includes some helper methods for working with datasets.
#
# The Veritable::VeritableResource and Veritable::VeritableObject modules are internal abstractions.
# 
# See also: https://dev.priorknowledge.com/docs/client/ruby
module Veritable

  # The HTTP User-Agent header used by the client library.
  USER_AGENT = 'veritable-ruby ' + VERSION

  # The default base URL for the Veritable API server.
  BASE_URL = "https://api.priorknowledge.com"
  
  # The main entry point to the Veritable API
  #
  # ==== Arguments
  # * +opts+ -- a Hash containing options for the connection. Possible keys include:
  #   - +:api_key+ -- the Veritable API key to use. If not set, defaults to <tt>ENV['VERITABLE_KEY']</tt>
  #   - +:api_base_url+ -- the base URL of the Veritable API. If not set, defaults to <tt>ENV['VERITABLE_URL']</tt> or BASE_URL
  #   - +:ssl_verify+ -- if +true+, the SSL certificate of the API server will be verified.
  #   - +:enable_gzip+ -- if +true+, requests to the API server will be gzipped.
  #
  # ==== Raises
  # A Veritable::VeritableError if no Veritable API server is found at the indicated URL.
  # 
  # ==== Returns
  # An instance of Veritable::API.
  #
  # See also: https://dev.priorknowledge.com/docs/client/ruby
  def self.connect(opts={})
    opts[:api_key] = opts[:api_key] || ENV['VERITABLE_KEY']
    opts[:api_base_url] = opts[:api_base_url] || ENV['VERITABLE_URL'] || BASE_URL

    opts[:ssl_verify] = true unless opts.has_key?(:ssl_verify)
    opts[:enable_gzip] = true unless opts.has_key?(:enable_gzip)

    begin
        api = API.new(opts)
        connection_test = api.root
        status = connection_test["status"]
        entropy = connection_test["entropy"]
        raise VeritableError.new("No Veritable server responding at #{opts[:api_base_url]}") if status != "SUCCESS"
        raise VeritableError.new("No Veritable server responding at #{opts[:api_base_url]}") if ! entropy.is_a?(Float)
    rescue Exception => e
        raise VeritableError.new("No Veritable server responding at #{opts[:api_base_url]}", {'inner_error' => e})
    end
    return api
  end
end
