# encoding: utf-8

require 'helper'

module Boolean; end
class TrueClass; include Boolean; end
class FalseClass; include Boolean; end


class TestVeritableRelated < Test::Unit::TestCase

  class << self
    def startup
		@api = Veritable.connect
		@t = @api.create_table
		@t.batch_upload_rows(
		[  {'_id' => 'row1', 'cat' => 'a', 'ct' => 0, 'real' => 1.02394, 'bool' => true},
		   {'_id' => 'row2', 'cat' => 'b', 'ct' => 0, 'real' => 0.92131, 'bool' => false},
		   {'_id' => 'row3', 'cat' => 'c', 'ct' => 1, 'real' => 1.82812, 'bool' => true},
		   {'_id' => 'row4', 'cat' => 'c', 'ct' => 1, 'real' => 0.81271, 'bool' => true},
		   {'_id' => 'row5', 'cat' => 'd', 'ct' => 2, 'real' => 1.14561, 'bool' => false},
		   {'_id' => 'row6', 'cat' => 'a', 'ct' => 5, 'real' => 1.03412, 'bool' => false}
		])
		@schema = Veritable::Schema.new({'cat' => {'type' => 'categorical'},
		  'ct' => {'type' => 'count'},
		  'real' => {'type' => 'real'},
		  'bool' => {'type' => 'boolean'}
		})
		@a = @t.create_analysis(@schema)
    end
    def shutdown
		@t.delete
    end
  end


  def setup
	@a = self.class.instance_variable_get :@a
	@schema = self.class.instance_variable_get :@schema
  end

  def teardown
  end

  def test_related_to
    @schema.keys.each {|col|
      assert @a.related_to(col).to_a.size <= 5
    }
    assert_raise(Veritable::VeritableError) {@a.related_to('missing_col')}
    assert @a.related_to('cat', {'start' => 'real'}).to_a.size <= 5
    assert @a.related_to('cat', {'limit' => 0}).to_a.size == 0
    assert @a.related_to('cat', {'limit' => 3}).to_a.size <= 3
    assert @a.related_to('cat', {'limit' => 3}).to_a.size <= 5
  end

end

