require 'test/unit'
require 'veritable'

class VeritableTest < Test::Unit::TestCase
	def test_connect
		Veritable.connect()
	end
end