# encoding: utf-8

require 'helper'

INVALIDS = ['éléphant', '374.34', 'ajfh/d/sfd@#$', 'きんぴらごぼう', '', ' foo', 'foo ', ' foo ', "foo\n", "foo\nbar", 3, 1.414, false, true, '_underscore', '-dash']

class VeritableRowOpTest < Test::Unit::TestCase
  def initialize(*args)
    @api = Veritable.connect
    super(*args)
  end
  
  def setup
    @t = @api.create_table
  end

  def teardown
    @t.delete
  end

  def test_upload_row_id
    @t.upload_row({'_id' => 'onebug', 'zim' => 'zam', 'wos' => 19.2})
  end

  def test_upload_row_id_json_roundtrip
    id = MultiJson.decode(MultiJson.encode(
            {'id' => Veritable::Util.make_table_id }))['id']
    @t.upload_row({'_id' => id, 'zim' => 'zam', 'wos' => 19.2})
  end

  def test_upload_row_invalid_id
    INVALIDS.each {|id|
      assert_raise(VeritableError, "ID #{id} passed") {
        @t.upload_row({'_id' => id, 'zim' => 'zam', 'wos' => 19.2})}
    }
  end

  def test_upload_row_autogen_id
    assert_raise(VeritableError) { @t.upload_row({'zim' => 'zom', 'wos' => 21.1})}
  end

  def test_upload_duplicate_rows
    @t.upload_row({'_id' => 'twobug', 'zim' => 'vim', 'wos' => 11.3})
    @t.upload_row({'_id' => 'twobug', 'zim' => 'fop', 'wos' => 17.5})
  end

  def test_batch_upload_rows
    id = MultiJson.decode(MultiJson.encode({'id' => "sevenbug"}))['id']
    @t.batch_upload_rows(
        [{'_id' => 'fourbug', 'zim' => 'zop', 'wos' => 10.3},
         {'_id' => 'fivebug', 'zim' => 'zam', 'wos' => 9.3},
         {'_id' => 'sixbug', 'zim' => 'zop', 'wos' => 18.9},
         {'_id' => id, 'zim' => 'zop', 'wos' => 14.9}])
  end

  def test_batch_upload_rows_invalid_ids
    rows = INVALIDS.inject([]) {|memo, id|
      memo << {'_id' => id, 'zim' => 'zop', 'wos' => 10.3}
    }
    assert_raise(VeritableError) {@t.batch_upload_rows rows}
  end

  def test_batch_upload_rows_missing_ids
    assert_raise(VeritableError) {
      @t.batch_upload_rows [{'zim' => 'zop', 'wos' => 10.3}, {'zim' => 'zam', 'wos' => 9.3},
             {'zim' => 'zop', 'wos' => 18.9},
             {'_id' => 'sixbug', 'zim' => 'fop', 'wos' => 18.3}]
    }
  end

  def test_batch_upload_rows_multipage
    [1, 331, 1000, 1421, 2000].each {|nrows|
      t2 = @api.create_table
      rs = (1..nrows).collect {|i|
        {'_id' => "r" + i.to_s, 'zim' => 'zop', 'wos' => rand, 'fop' => rand(1000)}
      }
      t2.batch_upload_rows rs
      rowiter = t2.rows
      assert nrows == rowiter.count
      @t.batch_delete_rows rs.collect {|row| {'_id' => row['_id']}}
      @t.batch_upload_rows t2.rows
      assert nrows = @t.rows.count
      @t.batch_delete_rows rs.collect {|row| {'_id' => row['_id']}}
      t2.delete
    }
    rs = (1..10000).collect {|i|
        {'_id' => "r" + i.to_s, 'zim' => 'zop', 'wos' => rand, 'fop' => rand(1000)}
    }
    [0, -5, 2.31, "foo", false].each {|pp|
      assert_raise(VeritableError, "Failed on #{pp}") {@t.batch_upload_rows(rs, pp)}
    }
  end
end
