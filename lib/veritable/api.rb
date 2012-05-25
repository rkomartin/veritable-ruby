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
      Table.new(@opts, get("tables/#{table_id}"))
    end

    def create_table(table_id=nil, description='', force=false)
      if table_id.nil?
        autogen = true
        table_id = Util.make_table_id
      else
        autogen = false
        Util.check_id table_id
      end

      if has_table? table_id
        if autogen
          return create_table(nil, description, false)
        end
        if ! force
          raise VeritableError.new("Couldn't create table -- table with id #{table_id} already exists.")
        else
          delete_table table_id
        end
      end
      doc = post("tables", {:_id => table_id, :description => description})
      Table.new(@opts, doc)
    end

    def delete_table(table_id); delete("tables/#{table_id}"); end

    def inspect; to_s; end
    def to_s; "#<Veritable::API url='#{api_base_url}'>"; end

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

    def row(row_id); get("#{link('rows')}/#{row_id}"); end

    def rows(start=nil, limit=nil)
      Cursor.new({'collection' => link('rows'),
        'start' => start,
        'limit' => limit}.update(@opts))
    end

    def upload_row(row)
      Util.check_row row
      put "#{link('rows')}/#{row['_id']}", row
    end

    def batch_upload_rows(rows, per_page=100)
      batch_modify_rows('put', rows, per_page)
    end

    def delete_row(row_id)
      delete "#{link('rows')}/row_id"
    end

    def batch_delete_rows(rows, per_page=100)
      batch_modify_rows('delete', rows, per_page)
    end

    def analysis(analysis_id)
      Analysis.new(@opts, get("#{link('analyses')}/#{analysis_id}"))
    end

    def analyses
      Cursor.new({'collection' => link('analyses')}.update(@opts)) {|x| Analysis.new(@opts, x)}
    end

    def delete_analysis(analysis_id)
      delete "#{link('analyses')}/analysis_id"
    end

    def create_analysis(schema, analysis_id=nil, description="", force=false, analysis_type="veritable")
      if analysis_type != "veritable"
        if analysis_type.respond_to? :to_s
          raise VeritableError.new("Invalid analysis type #{analysis_type}.")
        else
          raise VeritableError.new("Invalid analysis type.")
        end
      end

      if analysis_id.nil?
        autogen = true
        analysis_id = Util.make_analysis_id
      else
        autogen = false
        Util.check_id analysis_id
      end

      if has_table? analysis_id
        if autogen
          return create_analysis(nil, description, false)
        end
        if ! force
          raise VeritableError.new("Couldn't create table -- table with id #{table_id} already exists.")
        else
          delete_table table_id
        end
      end
      doc = post("tables", {:_id => table_id, :description => description})
      Table.new(@opts, doc)

    end

    def inspect; to_s; end
    def to_s; "#<Veritable::Table _id='#{_id}'>"; end

    def _id; @doc['_id']; end
    def description; @doc['description']; end

    def has_analysis?(analysis_id)
      begin
        analysis analysis_id
      rescue
        false
      else
        true
      end
    end

    private

    def batch_modify_rows(action, rows, per_page=100)
      if not per_page.is_a? Fixnum or not per_page > 0
        raise VeritableError.new("Batch upload or delete must have integer page size greater than 0.")
        rows.each {|row| Util.check_row row}
        ct = (1..per_page).to_a.cycle
        batch = Array.new()
        ct.each { |ct|
          if rows.empty?
            if batch.size > 0
              post link('rows'), {'action' => action, 'rows' => batch}
            end
            break
          end
          batch.push rows.shift
          if ct == per_page
            post link('rows'), {'action' => action, 'rows' => batch}
            batch = Array.new()
          end
        }
      end
    end
  end

  class Analysis
    include VeritableResource

    def update; @doc = get link 'self'; end

    alias :rest_delete :delete
    def delete; rest_delete link 'self'; end

    def schema; Schema.new(get(link('schema'))); end

    def wait(max_time=nil, poll=2)
      elapsed = 0
      while running?
        sleep poll
        if not max_time.nil?
          elapsed += poll
          if elapsed > max_time
            raise VeritableError.new("Wait for analysis -- Maximum time of #{max_time} second exceeded.")
          end
        end
        update
      end
    end

    def predict(row, count=100)
      update if running?
      if succeeded?
        if not row.is_a? Hash
          raise VeritableError.new("Predict -- Must provide a row hash to make predictions.")
        end
        res = post(link('predict'), {'data' => row, 'count' => count})
        if not res.is_a? Array
          begin
            res.to_s
          rescue
            raise VeritableError.new("Predict -- Error making predictions: #{res}")
          else
            raise VeritableError.new("Predict -- Error making predictions.")
          end
        end
        Prediction.new(row, res, schema)
      elsif running?
        raise VeritableError.new("Predict -- Analysis with id #{_id} is still running and not yet ready to predict.")
      elsif failed?
        raise VeritableError.new("Predict -- Analysis with id #{_id} has failed and cannot predict.")
      else
        raise VeritableError.new("Predict -- Shouldn't be here -- please let us know at support@priorknowledge.com.")
      end
    end

    def related_to(column_id, start=nil, limit=nil)
      update if running?
      if succeeded?
        Cursor.new({'collection' => "#{link('analyses')}/#{column_id}"}.update(@opts))
      elsif running?
        raise VeritableError.new("Related -- Analysis with id #{_id} is still running and not yet ready to calculate related.")
      elsif failed?
        raise VeritableError.new("Related -- Analysis with id #{_id} has failed and cannot calculate related.")
      else
        raise VeritableError.new("Related -- Shouldn't be here -- please let us know at support@priorknowledge.com.")
      end
    end

    def inspect; to_s; end
    def to_s; "#<Veritable::Analysis _id='#{_id}'>"; end

    def _id; @doc['_id']; end
    def created_at; @doc['created_at']; end
    def finished_at; @doc['finished_at']; end
    def state; @doc['state']; end
    def running?; state == 'running'; end
    def succeeded?; state == 'succeeded'; end
    def failed?; state == 'failed'; end
    def error; state == 'failed' ? @doc['error'] : nil; end
    def progress; state == 'succeeded' ? @doc['progress'] : nil; end
  end

class Schema < Hash
  def initialize(data, subset=nil)
    data.each {|k, v|
      if subset.is_a? Array
        self[k] = v if subset.include? k
      elsif subset.is_a? Hash
        self[k] = v if subset.has_key? k
      else
        self[k] = v
      end
    }
  end

  def validate
#      FIXME
  end
end

  class Prediction
    attr_reader :request
    attr_reader :distribution
    attr_reader :schema

    def initialize(request, distribution, schema)
      @request = row
      @distribution = res
      @schema = Schema.new(request.keys)
    end
  end
end
