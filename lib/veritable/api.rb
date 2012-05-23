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
      doc = post("tables", {:_id => table_id, :description => description})
      Table.new(@opts, doc)
    end

    def delete_table(table_id); delete("tables" + table_id); end

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

    def row(row_id); get(link('rows') + "/" + row_id); end

    def rows(start=nil, limit=nil)
      Cursor.new({'collection' => link('rows'),
        'start' => start,
        'limit' => limit}.update(@opts))
    end

    def upload_row(row)
#      FIXME
    end

    def batch_upload_rows(rows, per_page=100)
#      FIXME
    end

    def delete_row(row)
#      FIXME
    end

    def batch_delete_rows(rows, per_page=100)
#      FIXME
    end

    def analysis(analysis_id)
#      FIXME
    end

    def analyses
#      FIXME
    end

    def delete_analysis
#      FIXME
    end

    def create_analysis
#      FIXME
    end

    def inspect; to_s; end
    def to_s; "#<Veritable::Table _id='" + _id + "'>"; end

    def _id; @doc['_id']; end

    private

    def batch_modify_rows(rows, per_page=100)
#      FIXME
    end

    def has_analysis?(analysis_id)
      begin
        analysis analysis_id
      rescue
        false
      else
        true
      end
    end
  end

  class Analysis
    include VeritableResource

    def update; @doc = get link 'self'; end

    alias :rest_delete :delete
    def delete; rest_delete link 'self'; end

    def schema; get link 'schema'; end

    def wait(max_time=nil, poll=2)
      elapsed = 0
      while state == 'running'
#        FIXME
      end
    end

    def predict(row, count=100)
#      FIXME
    end

    def related_to(column_id, start=nil, limit=nil)
#      FIXME
    end

    def inspect; to_s; end
    def to_s; "#<Veritable::Analysis _id='" + _id + "'>"; end

    def _id; @doc['_id']; end
    def created_at; @doc['created_at']; end
    def finished_at; @doc['finished_at']; end
    def state; @doc['state'] end
    def error; state == 'failed' ? @doc['error'] : nil; end
    def progress; state == 'succeeded' ? @doc['progress'] : nil; end
  end

  class Schema
    def validate
#      FIXME
    end
  end

  class Prediction
  end
end
