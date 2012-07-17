require 'veritable/cursor'
require 'veritable/datatypes'
require 'veritable/errors'
require 'veritable/resource'
require 'veritable/util'

module Veritable


  # Represents the resources available to a user of the Veritable API.
  #
  # Users should not initialize directly; use Veritable.connect as the entry point.
  #
  # ==== Methods
  # * +root+ -- gets the root of the API
  # * +limits+ -- gets the user-specific API limits
  # * +tables+ -- gets a Veritable::Cursor over the collection of available tables
  # * +table+ -- gets an individual data table by its unique id
  # * +create_table+ -- creates a new data table
  # * +delete_table+ -- deletes a new data table by its unique id
  # * +has_table?+ -- checks whether a table with the given id is available
  # 
  # See also: https://dev.priorknowledge.com/docs/client/ruby  
  class API
    include VeritableResource

    def initialize(opts=nil, doc=nil)
      super
      @opts['api_limits'] = get("user/limits")
    end
    
    # Gets the root of the api
    #
    # ==== Returns
    # A Hash with the keys <tt>"status"</tt> (should be equal to <tt>"SUCCESS"</tt>) and <tt>"entropy"</tt> (a random Float).
    # 
    # See also: https://dev.priorknowledge.com/docs/client/ruby      
    def root; get(""); end

    # Gets the user-specific API limits
    #
    # ==== Returns
    # A Hash representing the user's current API limits.
    # 
    # See also: https://dev.priorknowledge.com/docs/client/ruby      
    def limits
        return @opts['api_limits']
    end

    # Gets a cursor for the table collection
    #
    # ==== Arguments
    # * +opts+ A Hash optionally containing the keys
    #   - <tt>"start"</tt> -- the table id from which the cursor should begin returning results. Defaults to +nil+, in which case the cursor will return result starting with the lexicographically first table id.
    #   - <tt>"limit"</tt> -- the total number of results to return (must be a Fixnum). Defaults to +nil+, in which case the number of results returned will not be limited.
    #
    # ==== Returns
    # A Veritable::Cursor. The cursor will return Veritable::Table objects representing the available data tables, in lexicographic order of their unique ids.
    # 
    # See also: https://dev.priorknowledge.com/docs/client/ruby      
    def tables(opts={'start' => nil, 'limit' => nil})
      Cursor.new({'collection' => "tables",
        'start' => opts['start'],
        'limit' => opts['limit']}.update(@opts)) {|x| Table.new(@opts, x)}
    end

    # Gets an individual table by its unique id
    #
    # ==== Arguments
    # * +table_id+ -- the unique id of the table
    #
    # ==== Returns
    # A Veritable::Table
    #
    # See also: https://dev.priorknowledge.com/docs/client/ruby      
    def table(table_id); Table.new(@opts, get("tables/#{table_id}")); end

    # Creates a new table
    #
    # ==== Arguments
    # * +table_id+ -- the unique String id of the new table. Must contain only alphanumeric characters, underscores, and dashes. Note that underscores and dashes are not permitted as the first character of a +table_id+. Default is +nil+, in which case a new id will be automatically generated.
    # * +description+ -- a String describing the table. Default is <tt>''</tt>.
    # * +force+ -- if true, will overwrite any existing table with the same id. Default is +false+.
    #
    # ==== Raises
    # A Veritable::VeritableError if +force+ is not true and there is an existing table with the same id.
    #
    # ==== Returns
    # A Veritable::Table
    #
    # See also: https://dev.priorknowledge.com/docs/client/ruby      
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

    # Deletes an existing table
    #
    # ==== Arguments
    # +table_id+ --- the unique id of the table to delete
    #
    # ==== Returns
    # +nil+ on success. Succeeds silently if no table with the specified id is found.
    #
    # See also: https://dev.priorknowledge.com/docs/client/ruby      
    def delete_table(table_id); delete("tables/#{table_id}"); nil; end

    # Checks if a table with the given unique id exists
    #
    # ==== Arguments
    # +table_id+ --- the unique id of the table to check
    #
    # ==== Returns
    # +true+ or +false+, as appropriate.
    #
    # See also: https://dev.priorknowledge.com/docs/client/ruby      
    def has_table?(table_id)
      begin
        table table_id
      rescue
        false
      else
        true
      end
    end

    # Returns a string representation of the API resource
    def inspect; to_s; end

    # Returns a string representation of the API resource
    def to_s; "#<Veritable::API url='#{api_base_url}'>"; end

  end

  # Represents the resources associated with a single table
  #
  # ==== Attributes
  # * +_id+ -- the unique String id of the table
  # * +description+ -- the String description of the table
  #
  # ==== Methods
  # * +delete+ -- deletes the associated table resource
  # * +row+ -- gets a row of the table by its unique id
  # * +rows+ -- gets a Veritable::Cursor over the collection of rows in the table
  # * +upload_row+ -- uploads a new row to the table
  # * +batch_upload_rows+ -- batch uploads multiple rows to the table
  # * +delete_row+ -- deletes a row from the table by its unique id
  # * +batch_delete_rows+ -- batch deletes multiple rows from the table
  # * +analyses+ -- gets a Veritable::Cursor over the collection of analyses of the table
  # * +analysis+ -- gets a single analysis of the table by its unique id
  # * +create_analysis+ -- creates a new analysis of the table
  # * +delete_analysis+ -- deletes an analysis of the table by its unique id
  # * +has_analysis?+ -- checks if the table has an analysis with the given id
  # 
  # See also: https://dev.priorknowledge.com/docs/client/ruby  
  class Table
    include VeritableResource

    alias :rest_delete :delete

    # Deletes the table
    #
    # ==== Returns
    # +nil+ on success. Succeeds silently if the resource has already been deleted.
    #
    # See also: https://dev.priorknowledge.com/docs/client/ruby      
    def delete; rest_delete(link('self')); end

    # Gets a row by its unique id
    #
    # ==== Arguments
    # +row_id+ --- the unique id of the row to retrieve
    #
    # ==== Returns
    # A Hash representing the row, whose keys are column ids as Strings and whose values are data cells.
    #
    # See also: https://dev.priorknowledge.com/docs/client/ruby      
    def row(row_id); get("#{link('rows')}/#{row_id}"); end

    # Gets a cursor for the row collection
    #
    # ==== Arguments
    # * +opts+ A Hash optionally containing the keys
    #   - <tt>"start"</tt> -- the row id from which the cursor should begin returning results. Defaults to +nil+, in which case the cursor will return result starting with the lexicographically first table id.
    #   - <tt>"limit"</tt> -- the total number of results to return (must be a Fixnum). Defaults to +nil+, in which case the number of results returned will not be limited.
    #
    # ==== Returns
    # A Veritable::Cursor. The cursor will return Hashes representing the rows, in lexicographic order of their unique ids.
    # 
    # See also: https://dev.priorknowledge.com/docs/client/ruby      
    def rows(opts={'start' => nil, 'limit' => nil})
      Cursor.new({'collection' => link('rows'),
        'start' => opts['start'],
        'limit' => opts['limit']}.update(@opts))
    end

    # Uploads a new row to the table
    #
    # ==== Arguments
    # * +row+ -- a Hash repreenting the data in the row, whose keys are column ids as Strings. Must contain the key <tt>"_id"</tt>, whose value must be a String containing only alphanumeric characters, underscores, and hyphens, and must be unique in the table.
    #
    # ==== Raises
    # A Veritable::VeritableError if the row Hash is missing the <tt>"_id"</tt> field or is improperly formed.
    #
    # ==== Returns
    # +nil+ on success.
    # 
    # See also: https://dev.priorknowledge.com/docs/client/ruby      
    def upload_row(row)
      Util.check_row row
      put("#{link('rows')}/#{row['_id']}", row)
      nil
    end

    # Batch uploads multiple rows to the table
    #
    # ==== Arguments
    # * +rows+ -- an Enumerable of Hashes, each of which represents a row of the table. Each row must contain the key <tt>"_id"</tt>, whose value must be a String containing only alphanumeric characters, underscores, and hyphens, and must be unique in the table.
    # * +per_page+ -- optionally controls the number of rows to upload in each batch. Defaults to +100+.
    #
    # ==== Returns
    # +nil+ on success.
    #
    # See also: https://dev.priorknowledge.com/docs/client/ruby      
    def batch_upload_rows(rows, per_page=100); batch_modify_rows('put', rows, per_page); end

    # Deletes a row from the table
    #
    # ==== Arguments
    # * +row_id+ -- the unique String id of the row to delete
    #
    # ==== Returns
    # +nil+ on success. Succeeds silently if the row does not exist in the table.
    #
    # See also: https://dev.priorknowledge.com/docs/client/ruby      
    def delete_row(row_id); rest_delete("#{link('rows')}/#{row_id}"); nil; end

    # Batch deletes a list of rows from the table
    #
    # ==== Arguments
    # * +rows+ -- an Enumerable of Hashes, each of which represents a row of the table. Each row must contain the key <tt>"_id"</tt>, whose value must be a String containing only alphanumeric characters, underscores, and hyphens, and must be unique in the table. Any other keys will be ignored.
    # * +per_page+ -- optionally controls the number of rows to delete in each batch. Defaults to +100+.
    #
    # ==== Returns
    # +nil+ on success.
    #
    # See also: https://dev.priorknowledge.com/docs/client/ruby      
    def batch_delete_rows(rows, per_page=100)
      begin
        batch_modify_rows('delete', rows, per_page)
      rescue VeritableError => e
        if (not e.respond_to?(:http_code)) or (not (e.http_code == "404 Resource Not Found"))
          raise e
        end
      end
    end

    # Gets an analysis by its unique id
    #
    # ==== Arguments
    # * +analysis_id+ -- the unique id of the analysis to retrieve
    #
    # ==== Returns
    # A new Veritable::Analysis
    #
    # See also: https://dev.priorknowledge.com/docs/client/ruby      
    def analysis(analysis_id); Analysis.new(@opts, get("#{link('analyses')}/#{analysis_id}")); end

    # Gets a cursor for the analysis collection
    #
    # ==== Arguments
    # * +opts+ A Hash optionally containing the keys
    #   - <tt>"start"</tt> -- the analysis id from which the cursor should begin returning results. Defaults to +nil+, in which case the cursor will return result starting with the lexicographically first analysis id.
    #   - <tt>"limit"</tt> -- the total number of results to return (must be a Fixnum). Defaults to +nil+, in which case the number of results returned will not be limited.
    #
    # ==== Returns
    # A Veritable::Cursor. The cursor will return Veritable::Analysis objects, in lexicographic order of their unique ids.
    # 
    # See also: https://dev.priorknowledge.com/docs/client/ruby      
    def analyses(opts={'start' => nil, 'limit' => nil})
      Cursor.new({'collection' => link('analyses'),
        'start' => opts['start'],
        'limit' => opts['limit']}.update(@opts)) {|x| Analysis.new(@opts, x)}
    end

    # Deletes an analysis by its unique id
    #
    # ==== Arguments
    # * +analysis_id+ -- the unique String id of the analysis to delete
    #
    # ==== Returns
    # +nil+ on success. Succeeds silently if the analysis does not exist.
    #
    # See also: https://dev.priorknowledge.com/docs/client/ruby      
    def delete_analysis(analysis_id); rest_delete("#{link('analyses')}/#{analysis_id}"); nil; end

    # Creates a new analysis
    #
    # ==== Arguments
    # * +schema+ -- a schema describing the analysis to perform. Must be a Veritable::Schema object or a Hash of the form:
    #     {'col_1': {type: 'datatype'}, 'col_2': {type: 'datatype'}, ...}
    # where the specified datatype for each column is one of <tt>['real', 'boolean', 'categorical', 'count']</tt> and is valid for the column.
    # * +analysis_id -- the unique String id of the new analysis. Must contain only alphanumeric characters, underscores, and dashes. Note that underscores and dashes are not permitted as the first character of an +analysis_id+. Default is +nil+, in which case a new id will be automatically generated.
    # * +description+ -- a String describing the analysis. Default is <tt>''</tt>.
    # * +force+ -- if true, will overwrite any existing analysis with the same id. Default is +false+.
    # * +analysis_type+ -- defaults to, and must be equal to, <tt>"veritable"</tt>.
    #
    # ==== Raises
    # A Veritable::VeritableError if +force+ is not true and there is an existing table with the same id.
    #
    # ==== Returns
    # A Veritable::Table
    #
    # See also: https://dev.priorknowledge.com/docs/client/ruby      
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

      if has_analysis? analysis_id
        if autogen
          return create_analysis(nil, description, false)
        end
        if ! force
          raise VeritableError.new("Couldn't create table -- table with id #{analysis_id} already exists.")
        else
          delete_analysis analysis_id
        end
      end
      doc = post(link('analyses'), {:_id => analysis_id, :description => description, :type => analysis_type, :schema => schema})
      Analysis.new(@opts, doc)
    end

    # Checks if an analysis with the given unique id exists
    #
    # ==== Arguments
    # * +analysis_id+ --- the unique id of the table to check
    #
    # ==== Returns
    # +true+ or +false+, as appropriate.
    #
    # See also: https://dev.priorknowledge.com/docs/client/ruby      
    def has_analysis?(analysis_id)
      begin
        analysis analysis_id
      rescue
        false
      else
        true
      end
    end

    # Returns a string representation of the table resource
    def inspect; to_s; end

    # Returns a string representation of the table resource
    def to_s; "#<Veritable::Table _id='#{_id}'>"; end

    # The String unique id of the table resources
    def _id; @doc['_id']; end

    # The String description of the table resource
    def description; @doc['description']; end

    private

    # Abstracts the logic for batch deleting and batch retrieving rows
    #
    # Private method -- do not call directly. Instead, call batch_upload_rows or batch_delete_rows as appropriate.
    def batch_modify_rows(action, rows, per_page=100)
      if not per_page.is_a? Fixnum or not per_page > 0
        raise VeritableError.new("Batch upload or delete must have integer page size greater than 0.")
      end
      batch = []
      rows.each do |row|
        Util.check_row(row)
        batch.push(row)
        if batch.size == per_page
            post(link('rows'), {'action' => action, 'rows' => batch})    
            batch = []
        end        
      end
      if batch.size > 0
        post(link('rows'), {'action' => action, 'rows' => batch})
      end
    end
  end

  # Represents the resources associated with a single analysis
  #
  # ==== Attributes
  # * +_id+ -- the unique String id of the analysis
  # * +description+ -- the String description of the analysis
  # * +created_at+ -- a String timestamp recording the time the analysis was created
  # * +finished_at+ -- a String timestamp recording the time the analysis completd
  # * +state+ -- the state of the analysis, one of <tt>["running", "succeeded", "failed"]</tt>
  # * +running?+ -- +true+ if +state+ is <tt>"running"</tt>
  # * +succeeded?+ -- ++true+ if +state+ is <tt>"succeeded"</tt>
  # * +failed?+ -- +true+ if +state+ is <tt>"failed"</tt>
  # * +error+ -- a Hash containing details of the error that occurred, if +state+ is <tt>"failed"</tt>, otherwise +nil+
  # * +progress+ -- a Hash containing details of the analysis progress, if +state+ is <tt>"running"</tt>, otherwise +nil+ 
  # * +schema+ -- a Veritable::Schema describing the columns included in the analysis
  #
  # ==== Methods
  # * +update+ -- refreshes the local representation of the API resource
  # * +delete+ -- deletes the associated API resource
  # * +wait+ -- blocks until the analysis succeeds or fails
  # * +predict+ -- makes new predictions based on the analysis
  # * +related_to+ -- calculates column relatedness based on the analysis
  # 
  # See also: https://dev.priorknowledge.com/docs/client/ruby  
  class Analysis
    include VeritableResource

    # Refreshes the local representation of the analysis
    #
    # ==== Returns
    # +nil+ on success
    # 
    # See also: https://dev.priorknowledge.com/docs/client/ruby  
    def update; @doc = get(link('self')); nil; end

    # Alias the connection's delete method as rest_delete
    alias :rest_delete :delete

    # Deletes the associated analysis resource
    #
    # ==== Returns
    # +nil+ on success. Succeeds silently if the analysis has already been deleted.
    # 
    # See also: https://dev.priorknowledge.com/docs/client/ruby  
    def delete; rest_delete(link('self')); end

    # The schema describing the analysis
    #
    # ==== Returns
    # A new Veritable::Schema object describing the colums contained in the analysis.
    # 
    # See also: https://dev.priorknowledge.com/docs/client/ruby  
    def schema
        if @old_schema.nil?
            @old_schema = Schema.new(get(link('schema')))
        end
        return @old_schema        
    end

    # Blocks until the analysis succeeds or fails
    #
    # ==== Arguments
    # * +max_time+ -- the maximum time to wait, in seconds. Default is +nil+, in which case the method will wait indefinitely.
    # * +poll+ -- the number of seconds to wait between polling the API server. Default is +2+.
    #
    # ==== Returns
    # +nil+ on success.
    # 
    # See also: https://dev.priorknowledge.com/docs/client/ruby  
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

    # Makes predictions based on the analysis
    #
    # ==== Arguments
    # * +row+ -- a Hash representing the row whose missing values are to be predicted. Keys must be valid String ids of columns contained in the underlying table, and values must be either fixed (conditioning) values of an appropriate type for each column, or +nil+ for values to be predicted.
    # * +count+ -- optionally specify the number of samples from the predictive distribution to return. Defaults to +100+.
    #
    # ==== Returns
    # A Veritable::Prediction object
    # 
    # See also: https://dev.priorknowledge.com/docs/client/ruby  
    def predict(row, count=100)
      if not row.is_a? Hash
        raise VeritableError.new("Predict -- Must provide a row hash to make predictions.")
      end
      return raw_predict([row], count, api_limits['predictions_max_response_cells'], api_limits['predictions_max_cols']).to_a[0]
    end
    

    # Makes predictions based on the analysis for multiple rows at a time
    #
    # ==== Arguments
    # * +rows+ -- an Enumerable of Hashes, each of which represents a row whose missing values are to be predicted. Keys must be valid String ids of columns contained in the underlying table, and values must be either fixed (conditioning) values of an appropriate type for each column, or +nil+ for values to be predicted. Each row Hash must also have a '_request_id' key with a unique string value.
    # * +count+ -- optionally specify the number of samples from the predictive distribution to return. Defaults to +100+.
    #
    # ==== Returns
    # An Enumerable of Veritable::Prediction objects
    # 
    # See also: https://dev.priorknowledge.com/docs/client/ruby  
    def batch_predict(rows, count=100)
      return raw_predict(PredictionsRequestCursor.new(rows), count, api_limits['predictions_max_response_cells'], api_limits['predictions_max_cols'])
    end

    
    # Scores how related columns are to a column of interest
    #
    # ==== Arguments
    # * +column_id+ -- the name of the column of interest
    # * +start+ -- the column name from which to start the cursor. Columns with related scores greater than or equal to the score of column +start+ will be returned by the cursor. Default is +nil+, in which case all columns in the table will be returned by the cursor.
    # * +limit+ -- optionally limits the number of columns returned by the cursor. Default is +nil+, in which case the number of columns returned will not be limited.
    #
    # ==== Returns
    # A Veritable::Cursor. The cursor will return column ids, in order of their relatedness to the column of interest.
    # 
    # See also: https://dev.priorknowledge.com/docs/client/ruby  
    def related_to(column_id, opts={'start' => nil, 'limit' => nil})
      update if running?
      if succeeded?
        Cursor.new(
         {'collection' => "#{link('related')}/#{column_id}",
          'start' => opts['start'],
          'limit' => opts['limit']}.update(@opts))
      elsif running?
        raise VeritableError.new("Related -- Analysis with id #{_id} is still running and not yet ready to calculate related.")
      elsif failed?
        raise VeritableError.new("Related -- Analysis with id #{_id} has failed and cannot calculate related.")
      else
        raise VeritableError.new("Related -- Shouldn't be here -- please let us know at support@priorknowledge.com.")
      end
    end

    # Returns rows which are similar to a target row in the context
    # of a particular column of interest.
    #
    # ==== Arguments
    # * +row+ -- either a row '_id' string or a row hash corrsponding to the target row. If a row hash is provided, it must contain an '_id' key whose value is the '_id' of a row present in the table at the time of the analysis
    # * +column_id+ -- the name of the column of interest.
    # * +max_rows+ -- the maximum number of similar rows to return. Default is +10+. The actual number of similar rows returned will be less than or equal to max_rows.
    # * +return_data+ -- if +true+, the full row content will be returned. If +false+, only the '_id' field for each row will be returned. Default is +true+.
    #
    # ==== Returns
    # An Enumerable of row entries ordered from most similar to least similar. 
    # Each row entry is an array with the first element being the row and 
    # the second element being a relatedness score between 0 to 1.
    # 
    # See also: https://dev.priorknowledge.com/docs/client/ruby  
    def similar_to(row, column_id, opts={:max_rows => 10, :return_data => true})
      if row.is_a? String
        row = {'_id' => row}
      end
      if not row.is_a? Hash
        raise VeritableError.new("Similar -- Must provide an existing row to get similar!")
      end
      update if running?
      if succeeded?
        doc = post(link('similar'), {:data => row, :column => column_id, 
                                     :max_rows => 10, :return_data => true}.update(opts))
        return SimilarCursor.new(doc['data'])
      elsif running?
        raise VeritableError.new("Similar -- Analysis with id #{_id} is still running and not yet ready to calculate similar.")
      elsif failed?
        raise VeritableError.new("Similar -- Analysis with id #{_id} has failed and cannot calculate similar.")
      else
        raise VeritableError.new("Similar -- Shouldn't be here -- please let us know at support@priorknowledge.com.")
      end
    end

    
    
    
    # Returns a string representation of the analysis resource
    def inspect; to_s; end

    # Returns a string representation of the analysis resource
    def to_s; "#<Veritable::Analysis _id='#{_id}'>"; end

    # The unique String id of the analysis
    def _id; @doc['_id']; end

    # String timestamp recording the time the analysis was created
    def created_at; @doc['created_at']; end

    # String timestamp recording the time the analysis completed
    def finished_at; @doc['finished_at']; end

    # The state of the analysis
    #
    # One of <tt>["running", "succeeded", "failed"]</tt>
    def state; @doc['state']; end

    # +true+ if +state+ is <tt>"running"</tt>, otherwise +false+
    def running?; state == 'running'; end

    # +true+ if +state+ is <tt>"succeeded"</tt>, otherwise +false+
    def succeeded?; state == 'succeeded'; end

    # +true+ if +state+ is <tt>"failed"</tt>, otherwise +false+
    def failed?; state == 'failed'; end

    # A Hash containing details of the error if +state+ is <tt>"failed"</tt>, otherwise +nil+
    def error; state == 'failed' ? @doc['error'] : nil; end

    # A Hash containing details of the analysis progress if +state+ is <tt>"running"</tt>, otherwise +nil+
    def progress; state == 'running' ? @doc['progress'] : nil; end

    # The String description of the analysis
    def description; @doc['description']; end
    
    private

    def raw_predict(rows, count, maxcells, maxcols)
        return PredictCursor.new(self, link('predict'), rows, count, maxcells, maxcols)
    end

    class PredictionsRequestCursor
        include Enumerable
        def initialize(rows)
            @rows = rows
        end
        def each
            @rows.each do |row|
                if not row.is_a? Hash
                    raise VeritableError.new("Predict -- Invalid row for predictions: #{row}")
                end
                if not row['_request_id'].is_a? String
                    raise VeritableError.new("Predict -- Rows for batch predictions must contain a string '_request_id' field: #{row}")
                end
                yield row
            end
        end
    end

    class PredictCursor
        include Enumerable
        
        def initialize(analysis, predict_link, rows, count, maxcells, maxcols)
            @analysis = analysis
            @predict_link = predict_link
            @rows = rows
            @count = count
            @maxcells = maxcells
            @maxcols = maxcols
        end
        
        def each
          rows = @rows
          count = @count
          maxcells = @maxcells
          maxcols = @maxcols          
          @analysis.update if @analysis.running?
          if @analysis.running?
            raise VeritableError.new("Predict -- Analysis with id #{_id} is still running and not yet ready to predict.")
          elsif @analysis.failed?
            raise VeritableError.new("Predict -- Analysis with id #{_id} has failed and cannot predict.")
          elsif @analysis.succeeded?
            ncells = 0
            batch = []
            rows.each {|row|
                ncols = (row.values.select {|v| v.nil?}).size
                tcols = (row.keys.select {|k| k != '_request_id'}).size
                if tcols > maxcols
                    raise VeritableError.new("Predict -- Cannot predict for row #{row['_request_id']} with more than #{maxcols} combined fixed and predicted values.")
                end
                if ncols > maxcells
                    raise VeritableError.new("Predict -- Cannot predict for row #{row['_request_id']} with #{ncols} missing values: exceeds predicted cell limit of #{maxcells}.")
                end
                n = ncols * count
                if (ncells + n) > maxcells
                    execute_batch(batch, count, maxcells).each {|p| yield p}
                    ncells = n
                    batch = [row]
                else
                    batch.push row
                    ncells = ncells + n
                end
            }
            execute_batch(batch, count, maxcells).each {|p| yield p}
          else
            raise VeritableError.new("Predict -- Shouldn't be here -- please let us know at support@priorknowledge.com.")
          end
        end
        
        def execute_batch(batch, count, maxcells)
            if batch.size == 0
                return []
            end
            if batch.size == 1
                data = batch[0]
                ncols = (data.values.select {|v| v.nil?}).size
                max_batch_count = (ncols == 0) ? count : (maxcells/ncols).to_i
                res = []
                while res.size < count do
                    batch_count = [max_batch_count, count - res.size].min
                    res = res + @analysis.post(@predict_link, {'data' => data, 'count' => batch_count, 'return_fixed' => false})
                end
            else
                res = @analysis.post(@predict_link, {'data' => batch, 'count' => count, 'return_fixed' => false})
            end
            if not res.is_a? Array
              begin
                res.to_s
              rescue
                raise VeritableError.new("Predict -- Error making predictions.")
              else
                raise VeritableError.new("Predict -- Error making predictions: #{res}")
              end
            end  
            preds = []
            (0...batch.size).each {|i|
                request = batch[i].clone
                request_id = request['_request_id']
                distribution = res[(i * count)...((i + 1) * count)]
                preds.push Prediction.new(request, distribution, @analysis.schema, request_id)
            }
            return preds
        end
        
    end
    
    class SimilarCursor
        include Enumerable
        def initialize(r)
          @r = r
        end
        def each
          @r.each do |x|
            yield x
          end
        end
    end

end


  # Represents a schema for a Veritable analysis
  #
  # A Veritable::Schema is a Hash with some additional convenience methods. Schema objects can be used interchangeably with Hashes of the same structure throughout veritable-ruby.
  #
  # ==== Methods
  # +type+ -- gets the datatype for a given column
  # +validate+ -- checks that the schema is well-formed
  # 
  # See also: https://dev.priorknowledge.com/docs/client/ruby  
  class Schema < Hash

    # Initalizes a new Schema from a Hash
    #
    # ==== Arguments
    # * +data+ -- the data for the schema as a Hash with the form:
    #     {'col_1': {type: 'datatype'}, 'col_2': {type: 'datatype'}, ...}
    # where the datatype must be one of <tt>["real", "categorical", "count", "boolean"]</tt>
    # * +subset+ -- a Hash or Array whose keys will be used to limit the columns present in the Schema created from the input +data+
    #
    # ==== Returns
    # A new Veritable::Schema
    #
    # See also: https://dev.priorknowledge.com/docs/client/ruby  
    def initialize(data, subset=nil)
      begin
        data.each {|k, v|
          if subset.is_a? Array
            self[k] = v if subset.include? k
          elsif subset.is_a? Hash
            self[k] = v if subset.has_key? k
          else
            self[k] = v
          end
        }
      rescue
        begin
          data.to_s
        rescue
          raise VeritableError.new("Initialize schema -- invalid schema data.")
        else
          raise VeritableError.new("Initialize schema -- invalid schema data #{data}.")
        end
      end
    end

    # Convenience accessor for the type of a Schema column
    #
    # Running <tt>schema.type(column)</tt> is sugar for <tt>schema[column]['type']</tt>
    #
    # ==== Arguments
    # +column+ -- the id of the column whose type we are retrieving
    #
    # See also: https://dev.priorknowledge.com/docs/client/ruby  
    def type(column); self[column]['type']; end

    # Validates the schema, checking that it is well-formed
    #
    # ==== Raises
    # A Veritable::VeritableError if any column ids or types are invalid.
    #
    # ==== Returns
    # +nil+ on success
    # 
    # See also: https://dev.priorknowledge.com/docs/client/ruby  
    def validate
      self.each {|k, v|
        if not k.is_a? String
          begin
            k.to_s
          rescue
            raise VeritableError.new("Validate schema -- Invalid schema specification: nonstring column id.")
          else
            raise VeritableError.new("Validate schema -- Invalid schema specification: nonstring column id #{k}")
          end
        end
        begin
          Util.check_id k
        rescue
          raise VeritableError.new("Validate schema -- Invalid column name #{k}: must contain only alphanumerics, dashes, and underscores, and may not begin with a dash or underscore.")
        end
        if not v.include? 'type'
          raise VeritableError.new("Validate schema -- Invalid schema specification. Column #{k} must specify a 'type', one of #{DATATYPES}")
        end
        if not DATATYPES.include? v['type']
          raise VeritableError.new("Validate schema -- Invalid schema specification. Column #{k}, type #{v['type']} is not valid. Type must be one of #{DATATYPES}")
        end
      }
      nil
    end
  end

# Represents the result of a Veritable prediction
#
# A Veritable::Prediction is a Hash whose keys are the columns in the prediction request, and whose values are standard point estimates for predicted columns. For fixed (conditioning) columns, the value is the fixed value. For predicted values, the point estimate varies by datatype:
# * real -- mean
# * count -- mean rounded to the nearest integer
# * categorical -- mode
# * boolean -- mode
# The object also gives access to the original predictions request, the predicted distribution on missing values, the schema of the analysis used to make predictions, and standard measures of uncertainty for the predicted values.
#
# ==== Attributes
# * +request+ -- a Hash containing the original predictions request. Keys are column names; conditioning values are present, predicted values are +nil+.
# * +distribution+ -- the underlying predicted distribution as an Array of Hashes, each of which represents a single sample from the predictive distribution.
# * +schema+ -- the schema for the columns in the predictions request
# * +uncertainty+ -- a Hash containing measures of uncertainty for each predicted value.
#
# ==== Methods
# * +prob_within+ -- calculates the probability a column's value lies within a range
# * +credible_values+ -- calculates a credible range for the value of a column
# 
# See also: https://dev.priorknowledge.com/docs/client/ruby  
  class Prediction < Hash
    # The original predictions request, as a Hash
    attr_reader :request

    # The original prediction '_request_id', nil if none was specified
    attr_reader :request_id
    
    # The underlying predicted distribution, as an Array of Hashes
    #
    # Each Hash represents a single draw from the predictive distribution, and should be regarded as equiprobable with the others.
    #
    # See also: https://dev.priorknowledge.com/docs/client/ruby  
    attr_reader :distribution

    # The schema for the columns in the predictions request
    attr_reader :schema

    # A Hash of standard uncertainty measures
    #
    # Keys are the columns in the prediction request and values are uncertainty measures associated with each point estimate. A higher value indicates greater uncertainty. These measures vary by datatype:
    # * real -- length of 90% credible interval
    # * count -- length of 90% credible interval
    # * categorical -- total probability of all non-modal values
    # * boolean -- probability of the non-modal value
    # 
    # See also: https://dev.priorknowledge.com/docs/client/ruby  
    attr_reader :uncertainty

    # Initializes a Veritable::Prediction
    #
    # Users should not call directly. Instead, call Veritable::Analysis#predict.
    # 
    # See also: https://dev.priorknowledge.com/docs/client/ruby  
    def initialize(request, distribution, schema, request_id=nil)
      @request = request
      @request.delete '_request_id'

      @schema = Schema.new(schema)
      @request_id = request_id

      fixed = {}
      @request.each { |k,v| 
        if not v.nil?
            fixed[k] = v
        end
      }
      @distribution = distribution
      @distribution.each {|d| 
        d.delete '_request_id'
        d.update(fixed)
      }

      @uncertainty = Hash.new()
      @request.each { |k,v|
        if v.nil?
          self[k] = point_estimate k
          @uncertainty[k] = calculate_uncertainty k
        else
          self[k] = v
          @uncertainty[k] = 0.0
        end
      }
      
      
    end
    
    # Calculates the probability a column's value lies within a range.
    #
    # Based on the underlying predicted distribution, calculates the marginal probability that the predicted value for the given columns lies within the specified range.
    #
    # ==== Arguments
    # column -- the column for which to calculate probabilities
    # range -- a representation of the range for which to calculate probabilities. For real and count columns, this is an Array of <tt>[start, end]</tt> representing a closed interval. For boolean and categorical columns, this is an Array of discrete values.
    #
    # ==== Returns
    # A probability as a Float
    #
    # See also: https://dev.priorknowledge.com/docs/client/python
    def prob_within(column, range)
      col_type = schema.type column
      Veritable::Util.check_datatype(col_type, "Probability within -- ")
      if col_type == 'boolean' or col_type == 'categorical'
        count = distribution.inject(0) {|memo, row|
          if range.include? row[column]
            memo + 1 
          else
            memo
          end
        }
        count.to_f / distribution.size
      elsif col_type == 'count' or col_type == 'real'
        mn = range[0]
        mx = range[1]
        count = distribution.inject(0) {|memo, row|
          v = row[column]
          if (mn.nil? or v >= mn) and (mx.nil? or v <=mx)
            memo + 1 
          else
            memo
          end
        }
        count.to_f / distribution.size
      end
    end

    # Calculates a credible range for the value of a column.

    # Based on the underlying predicted distribution, calculates a range within which the predicted value for the column lies with the specified probability.
    #
    # ==== Arguments
    # * +column+ -- the column for which to calculate the range
    # * +p+ -- The desired degree of probability. Default is +nil+, in which case will default to 0.5 for boolean and categorical columns, and to 0.9 for count and real columns.
    # 
    # ==== Returns
    # For boolean and categorical columns, a Hash whose keys are categorical values in the calculated range and whose values are probabilities; for real and count columns, an Array of the <tt>[min, max]</tt> values for the calculated range.
    #
    # See also: https://dev.priorknowledge.com/docs/client/ruby  
    def credible_values(column, p=nil)
      col_type = schema.type column
      Veritable::Util.check_datatype(col_type, "Credible values -- ")
      if col_type == 'boolean' or col_type == 'categorical'
        p = 0.5 if p.nil?
        tf = Hash.new
        ((freqs(counts(column)).sort_by {|k, v| v}).reject {|c, a| a < p}).each {|k, v| tf[k] = v}
        tf
      elsif col_type == 'count' or col_type == 'real'
        p = 0.9 if p.nil?
        n = distribution.size
        a = (n * (1.0 - p) / 2.0).round.to_i
        sv = sorted_values column
        n = sv.size
        lo = sv[a]
        hi = sv[n - 1 - a]
        [lo, hi]
      end
    end

    # Returns a string representation of the prediction results
    def inspect; to_s; end

    # Returns a string representation of the prediction results
    def to_s; "<Veritable::Prediction #{super}>"; end

    private

    # Private method: sorts the values for a column
    def sorted_values(column)
      values = (distribution.collect {|row| row[column]}).reject {|x| x.nil?}
      values.sort
    end

    # Private method: calculates counts for a column
    def counts(column)
      cts = Hash.new
      distribution.each {|row|
        if row.has_key? column
          cat = row[column]
          if not (cts.has_key? cat)
            cts[cat] = 0
          end
          cts[cat] += 1
        end
      }
      cts
    end

    # Private method: calculates frequencies for a column
    def freqs(cts)
      total = cts.values.inject(0) {|memo, obj| memo + obj}
      freqs = Hash.new()
      cts.each {|k, v|
        freqs[k] = v.to_f / total
      }
      freqs
    end

    # Private method: calculates point estimates for a column
    def point_estimate(column)
      col_type = schema.type column
      Veritable::Util.check_datatype(col_type, "Point estimate -- ")
      if col_type == 'boolean' or col_type == 'categorical'
        # use the mode
        (counts(column).max_by {|k, v| v})[0]
      elsif col_type == 'real' or col_type == 'count'
        # use the mean
        values = distribution.collect {|row| row[column]}
        mean = (values.inject(0) {|memo, obj| memo + obj}) / values.size.to_f
        col_type == 'real' ? mean : mean.round.to_i
      end
    end

    # Private method: calculates uncertainties for a column
    def calculate_uncertainty(column)
      values = distribution.collect {|row| row[column]}
      col_type = schema.type column
      Veritable::Util.check_datatype(col_type, "Calculate uncertainty -- ")
      n = values.size
      if col_type == 'boolean' or col_type == 'categorical'
        e = ((counts column).max_by {|k, v| v})[0]
        c = 1.0 - (values.count {|v| v == e} / n.to_f)
        c.to_f
      elsif col_type == 'count' or col_type == 'real'
        r = credible_values column
        (r[1] - r[0]).to_f
      end
    end
  end
end