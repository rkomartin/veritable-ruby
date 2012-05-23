require 'uuid'

module Veritable
  module Util
  	def make_table_id; UUID.new.generate :compact ; end
  	def make_analysis_id; UUID.new.generate :compact ; end
  end
end