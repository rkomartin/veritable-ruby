require 'veritable/object'
require 'multi_json'

module Veritable
  module Connection
    include VeritableObject
    def initialize(opts=nil, doc=nil)
      super(opts, doc)
      require_opts [:api_key, :api_url]
      default_opts({:ssl_verify => true, :enable_gzip => true})
    end

    def get(url, params=nil, headers={})
      if params and params.count > 0
        query_string = Util.query_params(params)
        url += "?#{query_string}"
      end
      request(:get, url, payload=nil, headers=headers)
    end

    def post(url, payload, headers={})
      request(:post, url, payload=payload, headers=headers)
    end

    def put(url, payload, headers={})
      request(:put, url, payload=payload, headers=headers)
    end

    def delete(url, headers={})
      request(:delete, url, payload=nil, headers=headers)
    end

    def request(verb, url, payload=nil, headers={})
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
      begin
        response = RestClient::Request.execute(opts)
      rescue RestClient::Exception => e
        begin
          r = MultiJson.decode(e.response)
          msg = r['message']
          code = r['code']
        rescue
          raise e
        end
        raise VeritableError.new("HTTP Error #{e.message} -- #{code}: #{msg}")
      end
      return MultiJson.decode(response)
    end

    private

    def api_key; @opts[:api_key]; end
    def api_base_url; @opts[:api_url]; end
    def ssl_verify; @opts[:ssl_verify]; end
    def enable_gzip; @opts[:enable_gzip]; end

  end
end
