require 'veritable/datatypes'
require 'veritable/errors'
require 'uuid'
require 'uri'
require 'csv'
require 'set'

module Veritable

  # Encapsulates utilities for working with data
  #
  # ==== Methods
  # * +read_csv+ -- reads a .csv from disk into an Array of row Hashes
  # * +write_csv+ -- writes an Array of row Hashes to disk as .csv
  # * +split_rows+ -- splits an Array of row Hashes into two sets
  # * +make_schema+ -- makes a new analysis schema from a schema rule
  # * +validate_data+ -- validates an Array of row Hashes against a schema
  # * +clean_data+ -- cleans an Array of row Hashes to conform to a schema
  # * +validate_predictions+ -- validates a single predictions request Hash against a schema
  # * +clean_predictions+ -- cleans a predictions request Hash to conform to a schema
  # * +validate_schema+ -- validates a schema
  # * +check_id+ -- checks that a unique ID is valid
  # * +check_row+ -- checks that a row Hash is well-formed
  # * +check_datatype+ -- checks that a datatype is valid
  # * +query_params+ -- helper function for HTTP form encoding
  # * +make_table_id+ -- autogenerates a new valid ID for a table
  # * +make_analysis_id+ -- autogenerates a new valid ID for an analysis
  # 
  # See also: https://dev.priorknowledge.com/docs/client/ruby  
  module Util
    class << self
      # Autogenerate a new table ID
      #
      # Users should not call directly
      def make_table_id; UUID.new.generate :compact ; end

      # Autogenerate a new analysis ID
      #
      # Users should not call directly
      def make_analysis_id; UUID.new.generate :compact ; end

      # Helper function for HTTP form encoding
      #
      # Users should not call directly
      def query_params(params, parent=nil)
        flatten_params(params).collect {|x|
          "#{x[0]}=#{x[1]}"
        }.join("&")
      end

      # Checks that a unique ID is valid
      #
      # Raises a VeritableError if the ID is invalid.
      def check_id(id)
        if not id.is_a? String
          begin
            id.to_s
          rescue
            raise VeritableError.new("Invalid id -- strings only.")
          else
            raise VeritableError.new("Invalid id '#{id}' -- strings only.")
          end
        elsif not id =~ Regexp.new('\A[a-zA-Z0-9][-_a-zA-Z0-9]*\z')
          raise VeritableError.new("Invalid id '#{id}' -- must contain only alphanumerics, underscores, and dashes.")
        elsif id[0] == '_' or id[0] == '-'
          raise VeritableError.new("Invalid id '#{id}' -- may not begin with a dash or underscore.")
        end
      end

      # Checks that a given row is well-formed
      #
      # Raises a VeritableError if the row Hash is not well-formed
      def check_row(row)
        if not row.is_a? Hash
          begin
            row.to_s
          rescue
            raise VeritableError.new("Invalid row -- Must provide a hash of column name-value pairs.")
          else
            raise VeritableError.new("Invalid row #{row} -- Must provide a hash of column name-value pairs.")
          end
        elsif not row.has_key? '_id'
          raise VeritableError.new("Invalid row #{row} -- rows must contain unique row ids in the '_id' field.")
        else
          begin
            check_id row['_id']
          rescue VeritableError => e
            raise VeritableError.new("Invalid row #{row} -- #{e}")
          end
        end
      end

      # Checks tht a given datatype is valid
      #
      # Raises a VeritableError if the datatype is invalid.
      def check_datatype(datatype, msg=nil)
        if not DATATYPES.include? datatype
          begin
            datatype.to_s
          rescue
            raise VeritableError.new("#{msg}Invalid data type.")
          else
            raise VeritableError.new("#{msg}Invalid data type '#{datatype}'.")
          end
        end
      end

      # Splits an array of row Hashes into two sets
      #
      # ==== Arguments
      # * +rows+ -- an Array of valid row Hashes
      # * +frac+ -- the fraction of the rows to include in the first set
      #
      # ==== Returns
      # An array <tt>[train_dataset, test_dataset]</tt>, each of whose members is an Array of row Hashes.
      # 
      # See also: https://dev.priorknowledge.com/docs/client/ruby  
      def split_rows(rows, frac)
        rows = rows.to_a
        n = rows.size
        inds = (0...n).to_a.shuffle
        border_ind = (n * frac).floor.to_i
        train_dataset = (0...border_ind).collect {|i| rows[inds[i]] }
        test_dataset = (border_ind...n).collect {|i| rows[inds[i]] }
        return [train_dataset, test_dataset]
      end

      # Validates a schema
      #
      # Checks that a Veritable::Schema or Hash of the appropriate form is well-formed.
      def validate_schema(schema); schema.is_a? Veritable::Schema ? schema.validate : Veritable::Schema.new(schema).validate; end

      # Makes a new analysis schema from a schema rule
      #
      # ==== Arguments
      # * +schema_rule+ -- a Hash or Array of two-valued Arrays, whose keys or first values should be regexes to match against column names, and whose values should be the appropriate datatype to assign to matching columns, for instance:
      #    [['a_regex_to_match', {'type' => 'continuous'}], ['another_regex', {'type' => 'count'}], ...]
      # * +opts+ -- a Hash which must contain either:
      #   - the key <tt>'headers'</tt>, whose value should be an Array of column names from which to construct the schema
      #   - or the key <tt>'rows'</tt>, whose value should be an Array of row Hashes from whose columns the schema is to be constructed
      #
      # ==== Returns
      # A new Veritable::Schema
      #
      # See also: https://dev.priorknowledge.com/docs/client/ruby  
      def make_schema(schema_rule, opts={})
        if ((not opts.has_key?('headers')) and (not opts.has_key?('rows')))
          raise VeritableError.new("Either 'headers' or 'rows' must be provided!")
        end
        headers = opts.has_key?('headers') ? opts['headers'] : nil
        if headers.nil?
          headers = Set.new
          opts['rows'].each {|row| headers.merge(row.keys)}
          headers = headers.to_a.sort
        end
        schema = {}
        headers.each do |c|
          schema_rule.each do |r, t|
            if r === c
              schema[c] = t
              break
            end
          end
        end
        return Veritable::Schema.new(schema)
      end

      # Writes an Array of row Hashes out to .csv
      #
      # ==== Arguments
      # * +rows+ -- an Array of valid row Hashes
      # * +filename+ -- a path to the .csv file to write out
      #
      # ==== Returns
      # +nil+ on success.
      #
      # See also: https://dev.priorknowledge.com/docs/client/ruby  
      def write_csv(rows, filename)
        headers = Set.new
        rows.each {|row| headers.merge(row.keys)}
        headers = headers.to_a.sort
        CSV.open(filename, "w") do |csv|
          csv << headers
          rows.each do |row|
            out_row = headers.collect {|h| row.keys.include?(h) ? row[h] : ''}
            csv << out_row
          end
        end
        nil
      end

      # Reads a .csv with headers in as an Array of row Hashes
      #
      # All values are kept as strings, except empty strings, which are omitted. To clean data and convert types in accordance with a given schema, use the clean_data and validate_data functions.
      #
      # ==== Arguments
      # * +filename+ -- a path to the .csv file to read in from
      # * +id_col+ -- optionally specify the column to rename to +'_id'+. If +nil+ (default) and a column named +'_id'+ is present, that column is used. If +nil+ and no +'_id'+ column is present, then +'_id'+ will be automatically generated.
      # * +na_cols+ -- a list of string values to omit; defaults to +['']+.
      #
      # ==== Returns
      # An Array of row Hashes
      #
      # See also: https://dev.priorknowledge.com/docs/client/ruby  
      def read_csv(filename, id_col=nil, na_vals=[''])
        rows = CSV.read(filename)
        header = rows.shift
        header = header.collect {|h| (h == id_col ? '_id' : h).strip}
        if header.include?('_id')
          id_col = '_id'
        end
        rid = 0
        rows = rows.collect do |raw_row|
          rid = rid + 1
          row = {}
          (0...raw_row.length).each do |i|
            row[header[i]] = ( na_vals.include?(raw_row[i]) ? nil : raw_row[i] )
          end
          if id_col.nil? 
            row['_id'] = rid.to_s
          end
          row
        end
        return rows
      end
        
      # Cleans up an Array of row Hashes in accordance with an analysis schema
      #
      # This method mutates its +rows+ argument. If clean_data raises an exception, values in some rows may be converted while others are left in their original state.
      #
      # ==== Arguments
      # * +rows+ -- the Array of Hashes to clean up
      # * +schema+ -- a Schema specifying the types of the columns appearing in the rows being cleaned
      # * +opts+ -- a Hash optionally containing the keys:
      #   - +convert_types+ -- controls whether clean_data will attempt to convert cells in a column to be of the correct type (default: +true+)
      #   - +remove_nones+ -- controls whether clean_data will automatically remove cells containing the value +nil+ (default: +true+)
      #   - +remove_invalids+ -- controls whether clean_data will automatically remove cells that are invalid for a given column (default: +true+)
      #   - +reduce_categories+ -- controls whether clean_data will automatically reduce the number of categories in categorical columns with too many categories (default: +true+) If +true+, the largest categories in a column will be preserved, up to the allowable limit, and the other categories will be binned as <tt>"Other"</tt>.
      #   - +assign_ids+ -- controls whether clean_data will automatically assign new ids to the rows (default: +false=) If +true+, rows will be numbered sequentially. If the rows have an existing <tt>'_id'</tt> column, +remove_extra_fields+ must also be set to +true+ to avoid raising a Veritable::VeritableError.
      #   - +remove_extra_fields+ -- controls whether clean_data will automatically remove columns that are not contained in the schema (default: +false+) If +assign_ids+ is +true+ (default), will also remove the <tt>'_id'</tt> column.
      #
      # ==== Raises
      # A Veritable::VeritableError containing further details if the data does not validate against the schema.
      #
      # ==== Returns
      # +nil+ on success (mutates +rows+ argument)
      # 
      # See also: https://dev.priorknowledge.com/docs/client/ruby
      def clean_data(rows, schema, opts={})
        validate(rows, schema, {
          'convert_types' => opts.has_key?('convert_types') ? opts['convert_types'] : true,
          'allow_nones' => false,
          'remove_nones' => opts.has_key?('remove_nones') ? opts['remove_nones'] : true,
          'remove_invalids' => opts.has_key?('remove_invalids') ? opts['remove_invalids'] : true,
          'reduce_categories' => opts.has_key?('reduce_categories') ? opts['reduce_categories'] : true,
          'has_ids' => true,
          'assign_ids' => opts.has_key?('assign_ids') ? opts['assign_ids'] : false,
          'allow_extra_fields' => true,
          'remove_extra_fields' => opts.has_key?('remove_extra_fields') ? opts['remove_extra_fields'] : false,
          'allow_empty_columns' => false})
      end

      # Validates an Array of row Hashes against an analysis schema
      #
      # ==== Arguments
      # * +rows+ -- the Array of Hashes to clean up
      # * +schema+ -- a Schema specifying the types of the columns appearing in the rows being cleaned
      #
      # ==== Raises
      # A Veritable::VeritableError containing further details if the data does not validate against the schema.
      #
      # ==== Returns
      # +nil+ on success
      # 
      # See also: https://dev.priorknowledge.com/docs/client/ruby
      def validate_data(rows, schema)
        validate(rows, schema, {
          'convert_types' => false,
          'allow_nones' => false,
          'remove_nones' => false,
          'remove_invalids' => false,
          'reduce_categories' => false,
          'has_ids' => true,
          'assign_ids' => false,
          'allow_extra_fields' => true,
          'remove_extra_fields' => false,
          'allow_empty_columns' => false})
      end

      # Cleans up a predictions request in accordance with an analysis schema
      #
      # This method mutates its +predictions+ argument. If clean_predictions raises an exception, values in some columns may be converted while others are left in their original state.
      #
      # ==== Arguments
      # * +predictions+ -- the predictions request to clean up
      # * +schema+ -- a Schema specifying the types of the columns appearing in the predictions request
      # * +opts+ -- a Hash optionally containing the keys:
      #   - +convert_types+ -- controls whether clean_data will attempt to convert cells in a column to be of the correct type (default: +true+)
      #   - +remove_invalids+ -- controls whether clean_data will automatically remove cells that are invalid for a given column (default: +true+)
      #   - +remove_extra_fields+ -- controls whether clean_data will automatically remove columns that are not contained in the schema (default: +true+)
      #
      # ==== Raises
      # A Veritable::VeritableError containing further details if the predictions request does not validate against the schema
      #
      # ==== Returns
      # +nil+ on success (mutates +predictions+ argument)
      # 
      # See also: https://dev.priorknowledge.com/docs/client/ruby
      def clean_predictions(predictions, schema, opts={})
        validate(predictions, schema, {
          'convert_types' => opts.has_key?('convert_types') ? opts['convert_types'] : true,
          'allow_nones' => true,
          'remove_nones' => false,
          'remove_invalids' => opts.has_key?('remove_invalids') ? opts['remove_invalids'] : true,
          'reduce_categories' => false,
          'has_ids' => false,
          'assign_ids' => false,
          'allow_extra_fields' => false,
          'remove_extra_fields' => opts.has_key?('remove_extra_fields') ? opts['remove_extra_fields'] : true,
          'allow_empty_columns' => true})
      end

      # Validates a predictions request against an analysis schema
      #
      # ==== Arguments
      # * +predictions+ -- the predictions request to clean up
      # * +schema+ -- a Schema specifying the types of the columns appearing in the predictions request
      #
      # ==== Raises
      # A Veritable::VeritableError containing further details if the predictions request does not validate against the schema.
      #
      # ==== Returns
      # +nil+ on success
      # 
      # See also: https://dev.priorknowledge.com/docs/client/ruby
      def validate_predictions(predictions, schema)
        validate(predictions, schema, {
          'convert_types' => false,
          'allow_nones' => true,
          'remove_nones' => false,
          'remove_invalids' => false,
          'reduce_categories' => false,
          'has_ids' => false,
          'assign_ids' => false,
          'allow_extra_fields' => false,
          'remove_extra_fields' => false,
          'allow_empty_columns' => true})
      end

      private

      # Private helper function for form encoding
      def flatten_params(params, parent=nil)
        result = []
        if params.is_a? Hash
          params.each {|k, v|
            kk = parent ? "#{parent}[#{urlencode(k)}]" : urlencode(k)
            if v.is_a?(Hash) or v.is_a?(Array)
              result += flatten_params(v, kk)
            else
              result << [kk, urlencode(v)]
            end
          }
        elsif params.is_a? Array
          params.each {|v|
            if v.is_a?(Hash) or v.is_a?(Array)
              result += flatten_params(v, kk)
            else
              result << ["#{parent}[]", urlencode(v)]
            end
          }
        end
        result
      end

      # Wraps URL encoding
      def urlencode(k); URI.escape(k.to_s, Regexp.new("[^#{URI::PATTERN::UNRESERVED}]")); end

      # Private helper function to convert to Integer
      def to_integer(v)
        return v if v.is_a? Fixnum
        v.gsub!(/\A([+-]?\d+?)\.0*?\Z/, '\1')
        Integer(v)
      end
        
      # Row validation logic
      #
      # Users should call clean_data, validate_data, clean_predictions, or validate_predictions
      def validate(rows, schema, opts)
        schema = Veritable::Schema.new(schema) unless schema.is_a? Veritable::Schema

        # ensure the schema is well-formed
        schema.validate  

        # store the row numbers of each unique id so that we can warn the user
        unique_ids = Hash.new

        # store the density of fields
        field_fill = Hash.new
        schema.keys.each {|c|
          field_fill[c] = 0 if c != '_id'
        }

        # store the number of categories in each categorical column
        category_counts = Hash.new

        # values which will be converted to true and false in boolean cols if convert_types
        true_strings = ['true', 't', 'yes', 'y']
        false_strings = ['false', 'f', 'no', 'n']

        max_cats = 256
        # be careful before changing the order of any of this logic -- the point is to do this all only once
        (0...rows.size).each {|i|
          if opts['assign_ids']
            rows[i]['_id'] = i.to_s  # number the rows sequentially
          elsif opts['has_ids']
            raise VeritableError.new("Validate -- row #{i} is missing key '_id'", {'row' => i, 'col' => '_id'}) unless rows[i].include? '_id'
            
            if opts['convert_types'] # attempt to convert _id to string
              begin
                rows[i]['_id'] = rows[i]['_id'].to_s if not rows[i]['_id'].is_a? String
              rescue
                raise VeritableError.new("Validate -- row #{i}, key '_id' cannot be converted to string.", {'row' => i, 'col' => '_id'})
              end
            end

            if not rows[i]['_id'].is_a? String # invalid type for _id
              begin
                rows[i]['_id'].to_s
              rescue
                raise VeritableError.new("Validate -- row #{i}, key '_id' is not a string.", {'row' => i, 'col' => '_id'})
              else
                raise VeritableError.new("Validate -- row #{i}, key '_id', value #{rows[i]['_id']} is not a string.", {'row' => i, 'col' => '_id'})
              end
            end
            
            begin
              check_id rows[i]['_id'] # make sure _id is alphanumeric
            rescue
              raise VeritableError.new("Validate -- row #{i}, key '_id', value #{rows[i]['_id']} contains disallowed characters. Ids must contain only alphanumerics, with underscores and hyphens allowed after the beginning of the id.", {'row' => i, 'col' => '_id'})
            end
            
            if unique_ids.include? rows[i]['_id']
              raise VeritableError.new("Validate -- row #{i}, key '_id', value #{rows[i]['_id']} is non-unique, conflicts with row #{unique_ids[rows[i]['_id']]}", {'row' => i, 'col' => '_id'})
            end
            
            unique_ids[rows[i]['_id']] = i
          elsif rows[i].include? '_id' # no ids, no autoid, but _id column
            if opts['remove_extra_fields'] # just remove it
              rows[i].delete '_id'
            else
              raise VeritableError.new("Validate -- row #{i}, key '_id' should not be included.", {'row' => i, 'col' => '_id'})
            end
          end
          rows[i].keys.each {|c|
            if c != '_id'
              if not schema.include? c # keys missing from schema
                if opts['remove_extra_fields'] # remove it
                  rows[i].delete c
                else
                  if not opts['allow_extra_fields'] # or silently allow
                    raise VeritableError.new("Row #{i}, key #{c} is not defined in schema", {'row' => i, 'col' => c})
                  end
                end
              elsif rows[i][c].nil? # nil values
                if opts['remove_nones'] # remove
                  rows[i].delete c
                else
                  if not opts['allow_nones'] # or silently allow
                    raise VeritableError.new("Row #{i}, key #{c} should be removed because it is nil", {'row' => i, 'col' => c})
                  end
                end
              else # keys present in schema
                coltype = schema.type c # check the column type
                if coltype == 'count'
                  if opts['convert_types'] # try converting to int
                    begin
                      rows[i][c] = to_integer(rows[i][c])
                    rescue
                      rows[i][c] = opts['remove_invalids'] ? nil : rows[i][c] # flag for removal
                    end
                  end
                  if rows[i][c].nil?
                    rows[i].delete c  # remove flagged values
                  elsif opts['remove_invalids'] and (rows[i][c].is_a? Fixnum) and (rows[i][c] < 0)
                    rows[i].delete c
                  else
                    if not (rows[i][c].is_a? Fixnum) or not (rows[i][c] >= 0) # catch invalids
                      raise VeritableError.new("Validate -- row #{i}, key #{c}, value #{rows[i][c]} is #{rows[i][c].class}, not a non-negative integer.", {'row' => i, 'col' => c})
                    end
                  end
                elsif coltype == 'real'
                  if opts['convert_types'] # try converting to float
                    begin
                      rows[i][c] = Float(rows[i][c]) unless rows[i][c].is_a? Float
                    rescue
                      rows[i][c] = opts['remove_invalids'] ? nil : rows[i][c] # flag for removal
                    end
                  end
                  if rows[i][c].nil?
                    rows[i].delete c
                  else
                    if not rows[i][c].is_a? Float
                      raise VeritableError.new("Validate -- row #{i}, key #{c}, value #{rows[i][c]} is a #{rows[i][c].class}, not a float.", {'row' => i, 'col' => c})
                    end
                  end
                elsif coltype == 'boolean'
                  if opts['convert_types'] # try converting to bool
                    lc = (rows[i][c]).to_s.strip.downcase
                    begin
                      if true_strings.include? lc
                        rows[i][c] = true
                      elsif false_strings.include? lc
                        rows[i][c] = false
                      elsif to_integer(rows[i][c]) == 0 # note that this behavior differs from what a rubyist might expect; "0" maps to false
                        rows[i][c] = false
                      else
                        rows[i][c] = true
                      end 
                    rescue
                      rows[i][c] = opts['remove_invalids'] ? nil : rows[i][c] # flag for removal
                    end
                  end
                  if rows[i][c].nil? # remove flagged values
                    rows[i].delete c
                  else
                    if not [true, false].include? rows[i][c]
                      raise VeritableError.new("Validate -- row #{i}, key #{c}, value #{rows[i][c]} is #{rows[i][c].class}, not a boolean", {'row' => i, 'col' => c})
                    end
                  end
                elsif coltype == 'categorical'
                  if opts['convert_types'] # try converting to string
                    begin
                      rows[i][c] = rows[i][c].to_s unless rows[i][c].is_a? String
                    rescue
                      rows[i][c] = opts['remove_invalids'] ? nil : rows[i][c] # flag for removal
                    end
                  end
                  if rows[i][c].nil? # remove flagged values
                    rows[i].delete c
                  else
                    if not rows[i][c].is_a? String # catch invalids
                      raise VeritableError.new("Validate -- row #{i}, key #{c}, value #{rows[i][c]} is a #{rows[i][c].class}, not a string", {'row' => i, 'col' => c})
                    end
                    category_counts[c] = Hash.new if not category_counts.include? c # increment count
                    category_counts[c][rows[i][c]] = 0 if not category_counts[c].include? rows[i][c]
                    category_counts[c][rows[i][c]] += 1
                  end
                else
                  raise VeritableError.new("Validate -- didn't recognize column type #{coltype}")
                end
              end
              if not field_fill.include? c and not opts['remove_extra_fields']
                field_fill[c] = 0
              end
              if rows[i].include? c and not rows[i][c].nil?
                field_fill[c] += 1
              end
            end
          }
        }
        category_counts.keys.each {|c|
          cats = category_counts[c].keys
          if cats.size > max_cats # too many categories
            if opts['reduce_categories'] # keep the largest max_cats - 1
              cats = cats.sort! {|a,b| category_counts[c][b] <=> category_counts[c][a]}
              category_map = Hash.new
              (0...cats.size).each {|j|
                j < max_cats - 1 ? category_map[cats[j]] = cats[j] : category_map[cats[j]] = "Other"
              }
              (0...rows.size).each {|i|
                rows[i][c] = category_map[rows[i][c]] if rows[i].include? c and not rows[i][c].nil?
              }
            else
              raise VeritableError.new("Validate -- categorical column #{c} has #{category_counts[c].keys.size} unique values which exceeds the limits of #{max_cats}.", {'col' => c})
            end
          end
        }
        if not opts['allow_empty_columns']
          field_fill.each {|c, fill|
            raise VeritableError.new("Validate -- column #{c} does not have any values", {'col' => c}) if fill == 0
          }
        end
        nil
      end
    end
  end
end
