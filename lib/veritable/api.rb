require 'veritable/cursor'
require 'veritable/resource'

module Veritable
  class API
    include VeritableResource
    def root
      get("")
    end

    def limits
      get("user/limits")
    end

    def tables
      Cursor.new({'collection' => "tables"}.update(@opts))
    end

    def table(name)
    end

    def inspect; to_s; end
    def to_s; "#<Veritable::API url='" + api_base_url + "'>"; end
  end

  class Table
    include VeritableResource
    def inspect; to_s; end
    def to_s; "#<Veritable::Table _id='" + _id + "'>"; end
  end

  class Analysis
    include VeritableResource
    def inspect; to_s; end
    def to_s; "#<Veritable::Analysis _id='" + _id + "'>"; end
  end

end
