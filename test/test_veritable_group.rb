# encoding: utf-8

require 'helper'

module Boolean; end
class TrueClass; include Boolean; end
class FalseClass; include Boolean; end


class TestVeritableGroup < Test::Unit::TestCase

  class << self
    def startup
        @api = Veritable.connect
        @t = @api.create_table
        @rows = [  {'_id' => 'row1', 'cat' => 'a', 'ct' => 0, 'real' => 1.02394, 'bool' => true},
           {'_id' => 'row2', 'cat' => 'b', 'ct' => 0, 'real' => 0.92131, 'bool' => false},
           {'_id' => 'row3', 'cat' => 'c', 'ct' => 1, 'real' => 1.82812, 'bool' => true},
           {'_id' => 'row4', 'cat' => 'c', 'ct' => 1, 'real' => 0.81271, 'bool' => true},
           {'_id' => 'row5', 'cat' => 'd', 'ct' => 2, 'real' => 1.14561, 'bool' => false},
           {'_id' => 'row6', 'cat' => 'a', 'ct' => 5, 'real' => 1.03412, 'bool' => false}
        ]
        @t.batch_upload_rows(@rows)
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
    @schema = self.class.instance_variable_get :@schema
    @rows = self.class.instance_variable_get :@rows
  end

  def teardown
  end

  def test_get_grouping
    @schema.keys.each {|col|
      g = @a.grouping(col)
      g.wait
      assert g.state == 'succeeded'
      assert g.column_id == col
    }
  end

  def test_get_groupings
    groupings = @a.groupings(@schema.keys.to_a)
    groupings.each {|g|
      g.wait
      assert g.state == 'succeeded'
    }
  end
  
  def test_grouping_groups
    g = @a.grouping('cat')
    g.wait()
    groups = g.groups.to_a
    rows = []
    groups.each {|gid|
      grows = g.rows({'group_id' => gid}).to_a
      rows = rows + grows
    }
    assert rows.count == @rows.count
    rows.each {|r|
      assert r.include? '_id'
      assert r.include? '_group_id'
      assert r.include? '_group_confidence'
    }
  end
  
  def test_grouping_rows
    g = @a.grouping('cat')
    g.wait()
    rows = g.rows.to_a
    assert rows.count == @rows.count
    rows.each {|r|
      assert r.include? '_id'
      assert r.include? '_group_id'
      assert r.include? '_group_confidence'
    }
  end

  def test_return_data_true
    g = @a.grouping('cat')
    g.wait()
    rows = g.rows({'return_data'=>true}).to_a
    assert rows.count == @rows.count
    rows.each {|r|
      assert r.include? '_id'
      assert r.include? '_group_id'
      assert r.include? '_group_confidence'
      assert r.include? 'cat'
    }
  end

  def test_return_data_false
    g = @a.grouping('cat')
    g.wait()
    rows = g.rows({'return_data'=>false}).to_a
    assert rows.count == @rows.count
    rows.each {|r|
      assert r.include? '_id'
      assert r.include? '_group_id'
      assert r.include? '_group_confidence'
      assert not r.include? 'cat'
    }
  end

  def test_get_row
    g = @a.grouping('cat')
    g.wait()
    r = g.row(@rows[0])
    assert r['_id'] == @rows[0]['_id']
    assert r.include? '_group_id'
    assert r.include? '_group_confidence'
  end

  
end

