require 'veritable/object'
require 'json'

module Veritable
  module Connection
    include VeritableObject
    def initialize(opts=nil, doc=nil)
      super(opts, doc)
      require_opts [:api_key, :api_url]
      default_opts({:ssl_verify => true, :enable_gzip => true})
    end

    def get(url, params=nil, headers={})
      request(:get, url, params=params, payload=nil, headers=headers)
    end

    def post(url, payload, headers={})
      request(:post, url, params=nil, payload=payload, headers=headers)
    end

    def put(url, payload, headers={})
      request(:put, url, params=nil, payload=payload, headers=headers)
    end

    def delete(url, headers={})
      request(:delete, url, params=nil, payload=nil, headers=headers)
    end

    def request(verb, url, params=nil, payload=nil, headers={})
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
