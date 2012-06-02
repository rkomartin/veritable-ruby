# encoding: utf-8

require 'helper'

class VeritableTableOpTest < Test::Unit::TestCase
  def initialize(*args)
    @api = Veritable.connect
    super(*args)
  end

  def setup
    @t = @api.create_table
    @rs = [
       {'_id' => 'onebug', 'zim' => 'zam', 'wos' => 19.2},
       {'_id' => 'twobug', 'zim' => 'vim', 'wos' => 11.3},
       {'_id' => 'threebug', 'zim' => 'fop', 'wos' => 17.5},
       {'_id' => 'fourbug', 'zim' => 'zop', 'wos' => 10.3},
       {'_id' => 'fivebug', 'zim' => 'zam', 'wos' => 9.3},
       {'_id' => 'sixbug', 'zim' => 'zop', 'wos' => 18.9}
      ]
    @t.batch_upload_rows @rs
    @t2 = @api.create_table
    @t2.batch_upload_rows(
      [{'_id' => 'row1', 'cat' => 'a', 'ct' => 0, 'real' => 1.02394, 'bool' => true},
       {'_id' => 'row2', 'cat' => 'b', 'ct' => 0, 'real' => 0.92131, 'bool' => false},
       {'_id' => 'row3', 'cat' => 'c', 'ct' => 1, 'real' => 1.82812, 'bool' => true},
       {'_id' => 'row4', 'cat' => 'c', 'ct' => 1, 'real' => 0.81271, 'bool' => true},
       {'_id' => 'row5', 'cat' => 'd', 'ct' => 2, 'real' => 1.14561, 'bool' => false},
       {'_id' => 'row6', 'cat' => 'a', 'ct' => 5, 'real' => 1.03412, 'bool' => false}
      ])
    @schema = Veritable::Schema.new({'zim' => {'type' => 'categorical'}, 'wos' => {'type' => 'real'}})
    @schema2 = Veritable::Schema.new({'cat' => {'type' => 'categorical'}, 'ct' => {'type' => 'count'}, 'real' => {'type' => 'real'}, 'bool' => {'type' => 'boolean'}})
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
    [[{'start' => 'onebug', 'limit' => 0}, 0],
     [{'start' => 'onebug', 'limit' => 3}, 3],
     [{'start' => 'onebug', 'limit' => 100}, 4]
   ].each {|test| 
      assert (@t.rows(test[0]).count == test[1])
    }
    [[{'start' => 'row0'}, 6],
     [{'start' => 'row7'}, 0]
    ].each {|test| 
      assert (@t2.rows(test[0]).count == test[1])
    }
  end

  def test_delete_row
    @t.delete_row('fivebug')
    @t.delete_row('fivebug')
    assert_raise(VeritableError) { @t.row('fivebug') }
  end

  def test_batch_delete_rows
    assert @t.rows.to_a.size == @rs.size
    @t.batch_delete_rows @rs
    assert @t.rows.to_a.size == 0
    @t.batch_upload_rows @rs
    assert @t.rows.to_a.size == @rs.size
    @t.batch_delete_rows @rs.collect {|r| {'_id' => r['_id']} }
    assert @t.rows.to_a.size == 0
  end

  def test_batch_delete_rows_some_deleted
    @rs << {'_id' => 'spurious'}
    @t.batch_delete_rows @rs
    assert @t.rows.to_a.size == 0
  end

  def test_batch_delete_rows_faulty
    rs = [{'zim' => 'zam', 'wos' => 9.3}, {'zim' => 'zop', 'wos' => 18.9}] + @rs
    assert_raise(VeritableError) {@t.batch_delete_rows rs}
  end

  def test_get_analyses
    a = @t.create_analysis(@schema)
    tid = a._id
    a.wait
    b = @t.create_analysis(@schema, analysis_id="zubble_1", description="An analysis", force=true)
    b.wait
    c = @t.create_analysis(@schema, analysis_id="zubble_2", description="An analysis", force=true)
    c.wait
    analyses = @t.analyses.to_a
    assert analyses.to_a.size == 3
    analyses.each {|a|
      assert a.is_a? Veritable::Analysis
    }
    assert @t.has_analysis? 'zubble_1'
    assert @t.has_analysis? tid
    assert @t.analysis('zubble_1').is_a? Veritable::Analysis
    assert @t.analysis(tid).is_a? Veritable::Analysis
  end

  def test_multiple_running_analyses_fail
    @t.create_analysis(@schema)
    assert_raise(VeritableError) { @t.create_analysis(@schema) }
  end

  def test_create_analysis_id_json_roundtrip
    id = MultiJson.decode(MultiJson.encode({'id' => 'zubble_2'}))['id']
    @t.create_analysis(@schema, analysis_id=id, description="An analysis", force=true)
    assert @t.analysis('zubble_2').is_a? Veritable::Analysis
  end

  def test_create_analysis_invalid_id
    INVALIDS.each {|tid|
      assert_raise(VeritableError) {@t.create_analysis(@schema, analysis_id=tid)}
    }
  end

  def test_create_duplicate_analysis
    a = @t.create_analysis(@schema, analysis_id="foo")
    a.wait
    assert_raise(VeritableError) {@t.create_analysis(@schema, analysis_id="foo")}
    @t.create_analysis(@schema, analysis_id="foo", description = "", force=true)
  end

  def test_create_analyses_malformed_schemata
    [{'zim' => {'type' => 'generalized_wishart_process'}, 'wos' => {'type' => 'ibp'}},
     'wimmel', {}, ['categorical', 'real'], true, false, 3, 3.143,
     {'zim' => {'type' => 'categorical'}, 'wos' => {'type' => 'real'}, 'krob' => {'type' => 'count'}}
    ].each {|s| assert_raise(VeritableError) {
      s = Veritable::Schema.new(s)
      @t.create_analysis(s) } }
  end

  def test_create_analysis_unpossible_type
    assert_raise(VeritableError) {@t.create_analysis(@schema, analysis_id="failing", description="should fail", force=true, analysis_type="svm")}
  end

  def test_wait_for_analysis
    a = @t.create_analysis(@schema)
    a.wait
    assert a.state == 'succeeded'
    s = Veritable::Schema.new({'zim' => {'type' => 'boolean'}, 'wos' => {'type' => 'real'}})
    a = @t.create_analysis(s)
    a.wait
    assert a.state == 'failed'
    assert a.error.is_a? Hash
    assert a.error['code'] == 'ANALYSIS_SCHEMA_INVALID_TYPE_FOR_COLUMN'
  end

  def test_create_analysis_all_datatypes
    a = @t2.create_analysis(@schema2)
    a.wait
    assert a.state == 'succeeded'
    assert a.created_at.is_a? String
    # assert a.finished_at.is_a? String
  end

  def test_create_analyses_datatype_mismatches
    {'cat' => 'real', 'cat' => 'count', 'cat' => 'boolean',
     'bool' => 'real', 'bool' => 'count', 'real' => 'count',
     'real' => 'categorical', 'real' => 'boolean', 'ct' => 'boolean',
     'ct' => 'categorical'}.each {|k, v|
      @schema2[k]['type'] = v
      a = @t2.create_analysis(@schema2)
      a.wait
      assert a.state == 'failed'
      assert a.error.is_a? Hash
      assert a.error['code'] == 'ANALYSIS_SCHEMA_INVALID_TYPE_FOR_COLUMN'
     }
  end

  def get_missing_analysis_fails
    assert_raise(VeritableError) {@t2.analysis 'yummy_tummy'}
  end

  def test_delete_analysis
    a = @t2.create_analysis(@schema2)
    a.delete
    assert ! @t2.has_analysis?(a._id)
    a.delete
    assert ! @t2.has_analysis?(a._id)
    a = @t2.create_analysis(@schema2)
    @t2.delete_analysis a._id
    assert ! @t2.has_analysis?(a._id)

  end

  def test_get_analysis_schema
    a = @t2.create_analysis(@schema2)
    s = a.schema
    assert s == @schema2
  end
end
