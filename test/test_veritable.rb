# encoding: utf-8

require 'test/unit'
require 'veritable'

class VeritableConnectTest < Test::Unit::TestCase
  def test_connect
    a = Veritable.connect
    assert a.is_a? Veritable::API
    assert a.to_s.is_a? String
  end

  def test_connect_unauthorized_fails
    assert_raise(VeritableError) { Veritable.connect({:api_key => "foo"}) }
  end

  def test_connect_nogzip
    a = Veritable.connect(opts={:enable_gzip => false})
    assert a.is_a? Veritable::API
  end

  def test_connect_no_ssl_verify
    a = Veritable.connect(opts={:ssl_verify => false})
    assert a.is_a? Veritable::API
  end

  def test_connect_no_json_failes
    assert_raise(MultiJson::DecodeError) { Veritable.connect({:api_url => "http://www.google.com"}) }
  end

  def test_connect_not_api_fails
    assert_raise(VeritableError) { Veritable.connect({:api_url => "https://graph.facebook.com/zuck"}) }
  end

  def test_instantiate_api
    a =Veritable::API.new({:api_key =>"foo", :api_url =>"bar"})
    assert a.is_a? Veritable::API
  end
end

class VeritableAPITest < Test::Unit::TestCase
  def setup
    @api = Veritable.connect
    @tid = Veritable::Util.make_table_id
  end

  def test_api_root
    r = @api.root
    assert r.is_a? Hash
    assert r['status'] == "SUCCESS"
    assert r['entropy'].is_a? Float
  end

  def test_api_limits
    l = @api.limits
    assert l.is_a? Hash
    %w{predictions_max_count max_string_length schema_max_cols
      max_row_batch_count max_categories predictions_max_cols table_max_rows
      table_max_cols_per_row table_max_running_analyses max_paginated_item_count}.each {|k| 
        assert l.has_key? k
      }
  end

  def test_api_list_tables
    tt = @api.tables
    assert tt.is_a? Veritable::Cursor
    assert tt.all? {|x| x.is_a? Veritable::Table}
  end

  def test_create_and_delete_table_autoid
    t = @api.create_table
    assert t.is_a? Veritable::Table
    assert @api.has_table? t._id
    @api.delete_table(t._id)
    assert ! @api.has_table?(t._id)
  end

  def test_create_table_with_id
    t = @api.create_table(@tid)
    assert t.is_a? Veritable::Table
    assert @api.has_table? @tid
    @api.delete_table @tid
  end

  def test_create_table_with_id_json_roundtrip
    tid = MultiJson.decode(MultiJson.encode({'id' => Veritable::Util.make_table_id}))['id']
    t = @api.create_table(tid)
    assert t.is_a? Veritable::Table
    assert @api.has_table? tid
    @api.delete_table tid
  end

  def test_create_table_invalid_id
    invalids = ['éléphant', '374.34', 'ajfh/d/sfd@#$', 'きんぴらごぼう', '', ' foo', 'foo ', ' foo ', "foo\n", "foo\nbar", 3, 1.414, false, true, '_underscore']
    invalids.each {|tid|
      assert_raise(VeritableError, "ID #{tid} passed") { @api.create_table tid}
    }
  end

  def test_create_table_description
    @api.create_table @tid, "A test table"
    @api.delete_table @tid
  end

  def test_get_table_by_id
    @api.create_table @tid, "A table"
    t = @api.table @tid
    assert t.is_a? Veritable::Table
    assert t.description == "A table"
    @api.delete_table @tid
  end

  def test_delete_deleted_table
    @api.create_table @tid
    @api.delete_table @tid
    @api.delete_table @tid
  end

  def test_create_deleted_table
    @api.create_table @tid
    @api.delete_table @tid
    @api.create_table @tid
    @api.delete_table @tid
    assert_raise(VeritableError) {@api.table @tid}
  end

  def test_create_duplicate_tables
    @api.create_table @tid
    assert_raise(VeritableError) {@api.create_table @tid}
    @api.delete_table @tid
  end

  def test_create_duplicate_tables_force
    @api.create_table @tid
    @api.create_table @tid, '', true
  end

  # FIXME inject to test autogen collision
end

class VeritableRowOpTest < Test::Unit::TestCase
  def setup
    @api = Veritable.connect
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
    invalids = ['éléphant', '374.34', 'ajfh/d/sfd@#$', 'きんぴらごぼう', '', ' foo', 'foo ', ' foo ', "foo\n", "foo\nbar", 3, 1.414, false, true, '_underscore']
    invalids.each {|id|
      assert_raise(VeritableError, "ID #{id} passed") {
        @t.upload_row({'_id' => id, 'zim' => 'zam', 'wos' => 19.2})}
    }
  end

  def test_upload_row_autogen_id
    assert_raise(VeritableError) { @t.upload_row({'zim' => 'zom', 'wos' => 21.1})}
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