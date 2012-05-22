require 'veritable/connection'

module Veritable
  class VeritableResource < Connection
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
    def to_s; "#<Veritable::API url='" + base_url + "'>"; end
  end
end
