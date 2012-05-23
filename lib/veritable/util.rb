require 'veritable/errors'
require 'uuid'
require 'uri'

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
        elsif not id =~ Regexp.new('\A[-_a-zA-Z0-9]+\z')
          raise VeritableError.new("Invalid id '#{id}' -- must contain only alphanumerics, underscores, and dashes.")
        elsif id[0] == '_' or id[0] == '-'
          raise VeritableError.new("Invalid id '#{id}' -- may not begin with a dash or underscore.")
        end
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
    end
  end
end