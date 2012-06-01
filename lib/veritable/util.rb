require 'veritable/datatypes'
require 'veritable/errors'
require 'uuid'
require 'uri'
require 'csv'
require 'set'

module Veritable
  module Util
    class << self
      def make_table_id; UUID.new.generate :compact ; end
      def make_analysis_id; UUID.new.generate :compact ; end

      def query_params(params, parent=nil)
        flatten_params(params).collect {|x|
          "#{x[0]}=#{x[1]}"
        }.join("&")
      end

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

      def split_rows(rows, frac=0.5)
        rows = rows.to_a
        n = rows.size
        inds = 0...n
        inds.shuffle!
        border_ind = (n * frac).floor.to_i
        train_dataset = (0...border_ind).collect {|i| rows[inds[i]] }
        test_dataset = (border_ind...n).collect {|i| rows[inds[i]] }
        return train_dataset, test_dataset
      end

      def validate_schema(schema)
        schema.is_a? Veritable::Schema ? schema.validate : Veritable::Schema.new(schema).validate
      end

      def make_schema(schema_rule, opts={})
        # construct an analysis schema from a schema rule (a list of lists)
      end

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
      end

      def read_csv(filename, id_col='_id', na_vals=[''])
        rows = CSV.read(filename)
        header = rows.shift
        header = header.collect {|h| (h == id_col ? '_id' : h).strip}
        rows = rows.collect do |raw_row|
          row = {}
          (0...raw_row.length).each do |i|
            row[header[i]] = ( na_vals.include?(raw_row[i]) ? nil : raw_row[i] )
          end
          row
        end
        return rows
      end
      
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

      def urlencode(k)
        URI.escape(k.to_s, Regexp.new("[^#{URI::PATTERN::UNRESERVED}]"))
      end
	  
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
                      rows[i][c] = Integer(rows[i][c])
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
                      elsif Integer(rows[i][c]) == 0
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
              cats = category_counts[c].sort.keys
              cats.reverse!
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
      end
    end
  end
end