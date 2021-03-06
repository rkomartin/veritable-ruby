require 'veritable/object'
require 'multi_json'

module Veritable

  # Encapsulates the HTTP logic for connecting to the Veritable API
  #
  # Users should not include this module.
  module Connection
    include VeritableObject

    # Initalizes a new connection
    def initialize(opts=nil, doc=nil)
      super(opts, doc)
      require_opts :api_key, :api_base_url
      default_opts(:ssl_verify => true, :enable_gzip => true)
    end

    # Wraps the HTTP GET logic
    def get(url, params=nil, headers={})
      if params and params.count > 0
        params.keys.to_a.each {|k|
            params.delete(k) if params[k].nil?
        }
        query_string = Util.query_params(params)
        url += "?#{query_string}"
      end
      request(:get, url, nil, headers)
    end

    # Wraps the HTTP POST logic
    def post(url, payload, headers={})
      payload = MultiJson.encode(payload)
      headers = headers.merge({:content_type => 'application/json'})
      request(:post, url, payload, headers)
    end

    # Wraps the HTTP PUT logic
    def put(url, payload, headers={})
      payload = MultiJson.encode(payload)
      headers = headers.merge({:content_type => 'application/json'})
      request(:put, url, payload, headers)
    end

    # Wraps the HTTP DELETE logic
    #
    # Silently allows DELETE of nonexistent resources
    def delete(url, headers={})
      begin
        request(:delete, url, nil, headers)
      rescue VeritableError => e
        if not e.respond_to? :http_code or not e.http_code == "404 Resource Not Found"
          raise e
        end
      end
    end

    # Wraps the core HTTP request logic
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
        raise VeritableError.new("HTTP Error #{e.message} -- #{code}: #{msg}", {'http_code' => e.message, 'api_code' => code, 'api_message' => msg})
      end
      return MultiJson.decode(response)
    end

    private

    # Private accessor for API key
    def api_key; @opts[:api_key]; end

    # Private accessor for API base URL
    def api_base_url; @opts[:api_base_url]; end

    # Private accessor for API :ssl_verify option
    def ssl_verify; @opts[:ssl_verify]; end

    # Private accessor for API :enable_gzip option
    def enable_gzip; @opts[:enable_gzip]; end

  end
end
