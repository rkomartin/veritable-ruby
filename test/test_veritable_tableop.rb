# encoding: utf-8

require 'test/unit'
require 'veritable'

INVALIDS = ['éléphant', '374.34', 'ajfh/d/sfd@#$', 'きんぴらごぼう', '', ' foo', 'foo ', ' foo ', "foo\n", "foo\nbar", 3, 1.414, false, true, '_underscore']

class VeritableTableOpTest < Test::Unit::TestCase
  def setup
    @api = Veritable.connect
    @t = @api.create_table
    @rs = [
       {'_id' => 'onebug', 'zim' => 'zam', 'wos' => 19.2},
       {'_id' => 'twobug', 'zim' => 'vim', 'wos' => 11.3},
       {'_id' => 'threebug', 'zim' => 'fop', 'wos' => 17.5},
       {'_id' => 'fourbug', 'zim' => 'zop', 'wos' => 10.3},
       {'_id' => 'fivebug', 'zim' => 'zam', 'wos' => 9.3},
       {'_id' => 'sixbug', 'zim' => 'zop', 'wos' => 18.9}
      ]
    @t.batch_upload_rows rs
    @t2 = @api.create_table
    @t2.batch_upload_rows(
      [{'_id' => 'row1', 'cat' => 'a', 'ct' => 0, 'real' => 1.02394, 'bool' => true},
       {'_id' => 'row2', 'cat' => 'b', 'ct' => 0, 'real' => 0.92131, 'bool' => false},
       {'_id' => 'row3', 'cat' => 'c', 'ct' => 1, 'real' => 1.82812, 'bool' => true},
       {'_id' => 'row4', 'cat' => 'c', 'ct' => 1, 'real' => 0.81271, 'bool' => true},
       {'_id' => 'row5', 'cat' => 'd', 'ct' => 2, 'real' => 1.14561, 'bool' => false},
       {'_id' => 'row6', 'cat' => 'a', 'ct' => 5, 'real' => 1.03412, 'bool' => false}
      ])
  end

  def teardown
    @t.delete
    @t2.delete
  end

  def test_get_row
    assert @t.row("sixbug") == {'_id' => 'sixbug', 'zim' => 'zop', 'wos' => 18.9}
    assert @t.row("fivebug") == {'_id' => 'fivebug', 'zim' => 'zam', 'wos' => 9.3}
  end

  def test_add_duplicate_rows
    @t.upload_row({'_id' => 'threebug', 'zim' => 'fop', 'wos' => 17.5})
    @t.upload_row({'_id' => 'threebug', 'zim' => 'vim', 'wos' => 11.3})
    assert @t.row("threebug") == {'_id' => 'threebug', 'zim' => 'vim', 'wos' => 11.3}
  end

  def test_batch_get_rows
    assert @t.rows.count == 6
  end

  def test_batch_get_rows_start
    assert @t.rows({'start' => 'onebug'}).count == 4
  end

  def test_batch_get_rows_limits
    {{'start' => 'onebug', 'limit' => 0} => 0,
     {'start' => 'onebug', 'limit' => 3} => 3,
     {'start' => 'onebug', 'limit' => 100} => 4,
     {'start' => 'row0'} => 6,
     {'start' => 'row7'} => 0
   }.each {|r, c| assert(@t.rows(r).count == c, "Failed on #{r}")}
  end

  def test_delete_row
    @t.delete_row('fivebug')
    @t.delete_row('fivebug')
    assert_raise(VeritableError) { @t.row('fivebug') }
  end

  def test_batch_delete_rows
    assert @t.rows.size == @rs.size
    @t.batch_delete_rows @rs
    assert @t.rows.size == 0
    @t.batch_upload_rows @rs
    assert @t.rows.size == @rs.size
    @t.batch_delete_rows @rs.collect {|r| {'_id' => r['_id']} }
    assert @t.rows.size == 0
  end

  def test_batch_delete_rows_some_deleted
    @rs << {'_id' => 'spurious'}
    @t.batch_delete_rows @rs
    assert @t.rows.size == 0
  end

  def test_batch_delete_rows_faulty
    rs = [{'zim' => 'zam', 'wos' => 9.3},
          {'zim' => 'zop', 'wos' => 18.9}] + @rs
    assert_raise(VeritableError) {@t.batch_delete_rows rs}
  end

  def test_get_analyses
  end

  def test_create_analysis_1
  end

  def test_create_analysis_2
  end

  def test_create_analysis_id_json_roundtrip
  end

  def test_create_analysis_invalid_id
  end

  def test_create_duplicate_analysis
  end

  def test_create_analyses_malformed_schemata
  end

  def test_create_analysis_unpossible_type
  end

  def test_wait_for_analysis_succeeds
  end

  def test_wait_for_analysis_fails
  end

  def test_error_analysis_failed
  end

  def test_create_analysis_all_datatypes
  end

  def test_create_analyses_datatype_mismatches
  end

  def get_created_analyses
  end

  def get_missing_analysis_fails
  end

  def test_delete_analysis
  end

  def test_get_analysis_schema
  end

end
