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