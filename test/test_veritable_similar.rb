# encoding: utf-8

require 'helper'

module Boolean; end
class TrueClass; include Boolean; end
class FalseClass; include Boolean; end


class TestVeritableSimilar < Test::Unit::TestCase

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
        @a.wait
    end
    def shutdown
        @t.delete
    end
  end


  def setup
    @a = self.class.instance_variable_get :@a
    @t = self.class.instance_variable_get :@t
    @schema = self.class.instance_variable_get :@schema
  end

  def teardown
  end

  def test_similar_to
    @schema.keys.each {|col|
      @t.rows.each {|row|
        s = @a.similar_to(row, col, opts={:max_rows => 1})
        assert s.size == 1
        assert s[0].size == 2
      }
    }
  end

  def test_similar_to_id_only
    s = @a.similar_to('row1','cat', opts={:max_rows => 3})
    assert s.size == 3
    assert s[0].size == 2
  end
  
  def test_similar_to_with_invalid_column_fails
    assert_raise(Veritable::VeritableError) {@a.similar_to('row1','missing-col', opts={:max_rows => 3})}    
  end

  def test_similar_to_with_invalid_row_fails
    assert_raise(Veritable::VeritableError) {@a.similar_to('missing-row','cat', opts={:max_rows => 3})}    
  end

  def test_similar_to_return_data
    s = @a.similar_to('row1','cat', opts={:max_rows => 1, :return_data => true})
    assert s.size == 1
    assert s[0].size == 2
    assert s[0][0].include? 'ct'
    s = @a.similar_to('row1','cat', opts={:max_rows => 1, :return_data => false})
    assert s.size == 1
    assert s[0].size == 2
    assert (not (s[0][0].include? 'ct'))
  end

  
end

