require 'json'

module Veritable
  class Connection
    def initialize(api_key, api_url, opts={})
      @api_key = api_key
      @api_base_url = api_url
      opts.has_key? :ssl_verify ? @ssl_verify = opts[:ssl_verify] : @ssl_verify = true
      opts.has_key? :enable_gzip ? @enable_gzip = opts[:enable_gzip] : @enable_gzip = true
    end

    def get(url, headers={})
      request(:get, url, headers=headers)
    end

    def post(url, payload, headers={})
      request(:post, url, payload=payload, headers=headers)
    end

    def put(url, payload, headers={})
      request(:put, url, payload=payload, headers=headers)
    end

    def delete(url, headers={})
      request(:delete, url, headers=headers)
    end

    def request(verb, url, api_key=nil, payload=nil, headers={}, opts={})
      url = @api_base_url + "/" + url
      api_key ||= @api_key

      opts.has_key? :ssl_verify ? ssl_verify = opts[:ssl_verify] : ssl_verify = @ssl_verify
      opts.has_key? :enable_gzip ? enable_gzip = opts[:enable_gzip] : enable_gzip = @enable_gzip

      headers = {
        :user_agent => USER_AGENT,
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
      return json.load(response)
    end
  end
end
