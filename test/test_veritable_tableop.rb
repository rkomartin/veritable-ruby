# encoding: utf-8

require 'test/unit'
require 'veritable'

INVALIDS = ['éléphant', '374.34', 'ajfh/d/sfd@#$', 'きんぴらごぼう', '', ' foo', 'foo ', ' foo ', "foo\n", "foo\nbar", 3, 1.414, false, true, '_underscore']

class VeritableTableOpTest < Test::Unit::TestCase
  def setup
    @api = Veritable.connect
    @t = @api.create_table
    @t.batch_upload_rows(
      [
       {'_id' => 'onebug', 'zim' => 'zam', 'wos' => 19.2},
       {'_id' => 'twobug', 'zim' => 'vim', 'wos' => 11.3},
       {'_id' => 'threebug', 'zim' => 'fop', 'wos' => 17.5},
       {'_id' => 'fourbug', 'zim' => 'zop', 'wos' => 10.3},
       {'_id' => 'fivebug', 'zim' => 'zam', 'wos' => 9.3},
       {'_id' => 'sixbug', 'zim' => 'zop', 'wos' => 18.9}
      ])
    @t2 = @api.create_table
    @t2.batch_upload_rows(
      [{'_id' => 'row1', 'cat' => 'a', 'ct' => 0, 'real' => 1.02394, 'bool' => True},
       {'_id' => 'row2', 'cat' => 'b', 'ct' => 0, 'real' => 0.92131, 'bool' => False},
       {'_id' => 'row3', 'cat' => 'c', 'ct' => 1, 'real' => 1.82812, 'bool' => True},
       {'_id' => 'row4', 'cat' => 'c', 'ct' => 1, 'real' => 0.81271, 'bool' => True},
       {'_id' => 'row5', 'cat' => 'd', 'ct' => 2, 'real' => 1.14561, 'bool' => False},
       {'_id' => 'row6', 'cat' => 'a', 'ct' => 5, 'real' => 1.03412, 'bool' => False}
      ])
  end

  def teardown
    @t.delete
    @t2.delete
  end

  def test_get_row
    assert @t.get_row("sixbug") == {'_id' => 'sixbug', 'zim' => 'zop', 'wos' => 18.9}
    assert @t.get_row("fivebug") == {'_id' => 'fivebug', 'zim' => 'zam', 'wos' => 9.3}
  end

  def test_add_duplicate_rows
    @t.upload_row({'_id' => 'threebug', 'zim' => 'fop', 'wos' => 17.5})
    @t.upload_row({'_id' => 'threebug', 'zim' => 'vim', 'wos' => 11.3})
    assert @t.get_row("threebug") == {'_id' => 'threebug', 'zim' => 'vim', 'wos' => 11.3}
  end

  def test_batch_get_rows
    assert @t.rows.count == 6
  end

  def test_batch_get_rows_start
    assert @t.rows({'start' => 'onebug'}).count == 4
  end

  def test_batch_get_rows_limits
    assert @t.rows({'start' => 'onebug', 'limit' => 0}).count == 0
    assert @t.rows({'start' => 'onebug', 'limit' => 3}).count == 3
    assert @t.rows({'start' => 'onebug', 'limit' => 100}).count == 4
    assert @t2.rows({'start' => 'row0'}).count == 6
    assert @t2.rows({'start' => 'row7'}).count == 0
  end
end

class VeritableTestConnection < Test::Unit::TestCase
  def test_instantiate
  end
end

class VeritableTestUtils < Test::Unit::TestCase
  def test_query_params
    [[{'foo' => 'bar', 'baz' => 2}, "foo=bar&baz=2"],
     [{'foo' => [1,2,3]}, "foo[]=1&foo[]=2&foo[]=3"],
     [{'foo' => {'a' => 1, 'b' => 2}}, "foo[a]=1&foo[b]=2"],
     [{'foo' => {'a' => 1, 'b' => [1,2,3]}}, "foo[a]=1&foo[b][]=1&foo[b][]=2&foo[b][]=3"]].each {|x|
      assert Veritable::Util.query_params(x[0]) == x[1]
    }
  end
end