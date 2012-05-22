require 'json'

module Veritable
  class Connection
    def initialize(opts=nil, doc=nil)
      @api_key = opts[:api_key]
      @api_base_url = opts[:api_url]
      opts.has_key?(:ssl_verify) ? @ssl_verify = opts[:ssl_verify] : @ssl_verify = true
      opts.has_key?(:enable_gzip) ? @enable_gzip = opts[:enable_gzip] : @enable_gzip = true
      @doc = doc
    end

    def base_url
      @api_base_url
    end

    def get(url, headers={})
      request(:get, url, payload=nil, headers=headers, opts={})
    end

    def post(url, payload, headers={})
      request(:post, url, payload=payload, headers=headers, opts={})
    end

    def put(url, payload, headers={})
      request(:put, url, payload=payload, headers=headers, opts={})
    end

    def delete(url, headers={})
      request(:delete, url, payload=nil, headers=headers, opts={})
    end

    def request(verb, url, payload=nil, headers={}, opts={})
      url = base_url + "/" + url
      opts.has_key?(:api_key) ? api_key = opts[:api_key] : api_key = @api_key
      opts.has_key?(:ssl_verify) ? ssl_verify = opts[:ssl_verify] : ssl_verify = @ssl_verify
      opts.has_key?(:enable_gzip) ? enable_gzip = opts[:enable_gzip] : enable_gzip = @enable_gzip

      headers = {
        :user_agent => USER_AGENT,
        :accept => :json,
        :accept_encoding => enable_gzip ? :gzip : nil
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
      return JSON.load(response)
    end
  end
end
