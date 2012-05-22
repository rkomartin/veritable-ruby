require 'json'

module Veritable
  class Connection
    def initialize(opts=nil, doc=nil)
      @opts = opts
      @doc = doc
      
      raise VeritableError unless @opts.has_key?(:api_key)
      raise VeritableError unless @opts.has_key?(:api_url)
      @opts[:ssl_verify] = true unless @opts.has_key?(:ssl_verify)
      @opts[:enable_gzip] = true unless @opts.has_key?(:enable_gzip)
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
      url = api_base_url + "/" + url

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

    private

    def api_key; @opts[:api_key]; end
    def api_base_url; @opts[:api_url]; end
    def ssl_verify; @opts[:ssl_verify]; end
    def enable_gzip; @opts[:enable_gzip]; end

  end
end
