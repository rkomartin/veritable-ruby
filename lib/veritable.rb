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
    api_key = opts[:api_key] || ENV['VERITABLE_KEY']
    api_url = opts[:api_url] || ENV['VERITABLE_URL'] || "https://api.priorknowledge.com"

    opts.has_key? :ssl_verify ? ssl_verify = opts[:ssl_verify] : ssl_verify = true
    opts.has_key? :enable_gzip ? enable_gzip = opts[:enable_gzip] : enable_gzip = true

    api = API.new(Connection.new(api_key, api_url, opts={:ssl_verify => ssl_verify,
      :enable_gzip => enable_gzip}))
    r = api.root
    api
  end
  
  class VeritableResource
    def initialize(connection=nil, doc=nil)
      @connection = connection
      @doc = doc
    end
  end

  class API < VeritableResource
    def root
      Veritable.request(:get, "", api_key)
    end

    def limits
      Veritable.request(:get, "user/limits", api_key)
    end

    def tables
    end

    def inspect; to_s; end
    def to_s; "#<Veritable::API url='" + @url + "'>"; end
  end
end
