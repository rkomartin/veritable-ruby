
require 'rubygems'
require 'openssl'

gem 'rest-client', '~> 1.4'
require 'rest_client'

require File.join(File.dirname(__FILE__), 'veritable/version')

module Veritable
  USER_AGENT = 'veritable-ruby ' + VERSION
  
  @@ssl_bundle_path = File.join(File.expand_path(File.dirname(__FILE__)),
    'data/ca-certificates.crt')
  @@ssl_verify = true 
  @@enable_gzip = true
  @@api_key = ENV['VERITABLE_KEY']
  @@api_base_url = ENV['VERITABLE_URL'] || "https://api.priorknowledge.com"

  def self.connect(opts={})
    api_key = opts[:api_key] || api_key
    api_url = opts[:api_url] || 
    API.new(Connection.new(api_key, api_url))
  end
  
  class VeritableResource
    def initialize(connection=nil, doc=nil)
      # pass the connection object each time we initialize
      # also the documents -- as parsed JSON
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
  
  def self.request(verb, url, api_key=nil, payload=nil, headers={}, opts={})
    url = @@api_base_url + "/" + url
    api_key ||= @@api_key

    if opts.has_key? :ssl_verify
      ssl_verify = opts[:ssl_verify] 
    else
      ssl_verify = @@ssl_verify
    end

    if opts.has_key? :enable_gzip
      enable_gzip = opts[:enable_gzip] ? :gzip : ""
    else
      enable_gzip = @@enable_gzip ? :gzip : ""
    end

    headers = {
      :user_agent => USER_AGENT
      :accept => :json,
      :accept_encoding => enable_gzip
    }.merge(headers)

    opts = {
      :method => verb.to_s,
      :url => url,
      :user => api_key,
      :password => "",
      :headers => headers,
      :payload => payload,
      :verify_ssl => ssl_verify,
    }
    response = RestClient::Request.execute(opts)
    return response
  end

  def self.api_key=(api_key); @@api_key = api_key; end
  def self.api_key; @@api_key; end
  def self.api_url=(api_url); @@api_url = api_url; end
  def self.api_url; @@api_url; end
end
