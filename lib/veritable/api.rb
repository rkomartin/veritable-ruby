require 'veritable/cursor'
require 'veritable/errors'
require 'veritable/resource'
require 'veritable/util'

module Veritable
  class API
    include VeritableResource
    def root; get(""); end

    def limits; get("user/limits"); end

    def tables
      Cursor.new({'collection' => "tables"}.update(@opts)) {|x| Table.new(@opts, x)}
    end

    def table(table_id)
      Table.new(@opts, get("tables/" + table_id))
    end

    def create_table(table_id=nil, description=nil, force=false)
      if ! table_id
        autogen = true
        table_id = Util.make_table_id
      else
        autogen = false
      end

      if has_table? table_id
        if autogen
          return create_table(nil, description, false)
        end
        if ! force
          raise VeritableError
        else
          delete_table table_id
        end
      end
      doc = post "tables" {:_id => table_id, :description => description}
      Table.new(@opts, doc)
    end

    def inspect; to_s; end
    def to_s; "#<Veritable::API url='" + api_base_url + "'>"; end

    private

    def has_table?(table_id)
      begin
        table table_id
      rescue
        false
      else
        true
      end
    end
  end

  class Table
    include VeritableResource

    alias :rest_delete :delete
    def delete
      rest_delete link 'self'
    end

    def inspect; to_s; end
    def to_s; "#<Veritable::Table _id='" + _id + "'>"; end

    private

    def _id; @doc['_id']; end
  end

  class Analysis
    include VeritableResource

    def inspect; to_s; end
    def to_s; "#<Veritable::Analysis _id='" + _id + "'>"; end

    private

    def _id; @doc['_id']; end
  end
end
