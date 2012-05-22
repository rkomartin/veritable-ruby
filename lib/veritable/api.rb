require 'veritable/connection'

module Veritable
  class VeritableResource
    include Connection

    def initialize(connection=nil, doc=nil)
      @api_key = connection[:api_key]
      @api_base_url = connection[:api_url]
      connection.has_key? :ssl_verify ? @ssl_verify = connection[:ssl_verify] : @ssl_verify = true
      connection.has_key? :enable_gzip ? @enable_gzip = connection[:enable_gzip] : @enable_gzip = true
      @doc = doc
    end
  end

  class API < VeritableResource
    def root
      get("")
    end

    def limits
      get("user/limits")
    end

    def tables
      get("tables")
    end

    def inspect; to_s; end
    def to_s; "#<Veritable::API url='" + @url + "'>"; end
  end
end