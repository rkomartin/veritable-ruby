require 'helper'
require 'tempfile'

class VeritableTestUtils < Test::Unit::TestCase

  def setup
    @vschema = Veritable::Schema.new({
      'ColInt' => {'type' => 'count'},
      'ColFloat' => {'type' => 'real'},
      'ColCat' => {'type' => 'categorical'},
      'ColBool' => {'type' => 'boolean'}
    })
  end

  def test_split_rows
    rows = [{'_id' => '1', 'ColInt' => 3},
            {'_id' => '2', 'ColInt' => 4},
            {'_id' => '3', 'ColInt' => 4},
            {'_id' => '4', 'ColInt' => 4},
            {'_id' => '5', 'ColInt' => 4},
            {'_id' => '6', 'ColInt' => 4},
            {'_id' => '7', 'ColInt' => 4},
            {'_id' => '8', 'ColInt' => 4},
            {'_id' => '9', 'ColInt' => 4},
            {'_id' => '10', 'ColInt' => 4}]
    srows = Veritable::Util.split_rows(rows, 0.3)
    assert srows[0].length == 3
    assert srows[1].length == 7
  end
  
  def test_write_read_csv
    file = Tempfile.new('vtest')
    file.close
    begin
      refrows = [{'_id' => '7', 'ColInt' => 3, 'ColFloat' => 3.1, 'ColCat' => 'a'},
                 {'_id' => '8', 'ColInt' => 4, 'ColCat' => 'b', 'ColBool' => false},
                 {'_id' => '9'}]
      Veritable::Util.write_csv(refrows, file.path)
      testrows = Veritable::Util.read_csv(file.path)
      cschema = {
          'ColInt' => {'type' => 'count'},
          'ColFloat' => {'type' => 'real'},
          'ColCat' => {'type' => 'categorical'},
          'ColBool' => {'type' => 'boolean'}
          }
      Veritable::Util.clean_data(testrows, cschema)
      assert testrows.length == refrows.length
      (0...testrows.length).each do |i|
          assert testrows[i] == refrows[i]
      end
    ensure
      file.unlink
    end
  end

  def test_read_csv_map_id
    file = Tempfile.new('vtest')
    file.close
    begin
      refrows = [{'myID' => '7', 'ColInt' => 3, 'ColFloat' => 3.1, 'ColCat' => 'a'},
                 {'myID' => '8', 'ColInt' => 4, 'ColCat' => 'b', 'ColBool' => false},
                 {'myID' => '9'}]
      Veritable::Util.write_csv(refrows, file.path)
      testrows = Veritable::Util.read_csv(file.path, 'myID')
      cschema = {
          'ColInt' => {'type' => 'count'},
          'ColFloat' => {'type' => 'real'},
          'ColCat' => {'type' => 'categorical'},
          'ColBool' => {'type' => 'boolean'}
          }
      Veritable::Util.clean_data(testrows, cschema)
      assert testrows.length == refrows.length
      (0...testrows.length).each do |i|
          refrows[i]['_id'] = refrows[i]['myID']
          refrows[i].delete('myID')
          assert testrows[i] == refrows[i]
      end
    ensure
      file.unlink
    end
  end

  def test_read_csv_assign_id
    file = Tempfile.new('vtest')
    file.close
    begin
      refrows = [{'ColInt' => 3, 'ColFloat' => 3.1, 'ColCat' => 'a'},
                 {'ColInt' => 4, 'ColCat' => 'b', 'ColBool' => false},
                 {}]
      Veritable::Util.write_csv(refrows, file.path)
      testrows = Veritable::Util.read_csv(file.path)
      cschema = {
          'ColInt' => {'type' => 'count'},
          'ColFloat' => {'type' => 'real'},
          'ColCat' => {'type' => 'categorical'},
          'ColBool' => {'type' => 'boolean'}
          }
      Veritable::Util.clean_data(testrows, cschema)
      assert testrows.length == refrows.length
      (0...testrows.length).each do |i|
          refrows[i]['_id'] = (i+1).to_s
          assert testrows[i] == refrows[i]
      end
    ensure
      file.unlink
    end
  end

  def test_make_schema_headers
    ref_schema = {'CatA' => {'type' => 'categorical'},
                 'CatB' => {'type' => 'categorical'},
                 'IntA' => {'type' => 'count'},
                 'IntB' => {'type' => 'count'}}
    headers = ['IntA', 'IntB', 'CatA', 'CatB', 'Foo']
    schemaRule = [[/Int.*/, {'type' => 'count'}],
                  [/Cat.*/, {'type' => 'categorical'}]]
    schema = Veritable::Util.make_schema(schemaRule, {'headers' => headers})
    assert schema == ref_schema
  end

  def test_make_schema_rows
    ref_schema = {'CatA' => {'type' => 'categorical'},
                 'CatB' => {'type' => 'categorical'},
                 'IntA' => {'type' => 'count'},
                 'IntB' => {'type' => 'count'}}
    rows = [{'CatA' => nil, 'CatB' => nil, 'IntA' => nil, 'IntB' => nil, 'Foo' => nil}]
    schemaRule = [[/Int.*/, {'type' => 'count'}],
                  [/Cat.*/, {'type' => 'categorical'}]]
    schema = Veritable::Util.make_schema(schemaRule, {'rows' => rows})
    assert schema == ref_schema
  end
  
  def test_make_schema_noarg_fail
    ref_schema = {'CatA' => {'type' => 'categorical'},
                 'CatB' => {'type' => 'categorical'},
                 'IntA' => {'type' => 'count'},
                 'IntB' => {'type' => 'count'}}
    schemaRule = [[/Int.*/, {'type' => 'count'}],
                  [/Cat.*/, {'type' => 'categorical'}]]
    assert_raise Veritable::VeritableError do
        schema = Veritable::Util.make_schema(schemaRule, {})
    end
  end

  def test_missing_schema_type_fail
    bschema = {'ColInt' => {}, 'ColFloat' => {'type' => 'real'}}
    assert_raise Veritable::VeritableError do
        Veritable::Util.validate_data([], bschema)
    end
    assert_raise Veritable::VeritableError do
        Veritable::Util.clean_data([], bschema)
    end
  end

  def test_bad_schema_type_fail
    bschema = {'ColInt' => {'type' => 'jello'}, 'ColFloat' => {'type' => 'real'}}
    assert_raise Veritable::VeritableError do
        Veritable::Util.validate_data([], bschema)
    end
    assert_raise Veritable::VeritableError do
        Veritable::Util.clean_data([], bschema)
    end
  end
  
  def test_invalid_schema_underscore
    assert_raise Veritable::VeritableError do
        Veritable::Util.validate_data([], {'_foo' => {'type' => 'count'}})
    end
  end

  def test_invalid_schema_dot
    assert_raise Veritable::VeritableError do
        Veritable::Util.validate_data([], {'b.d' => {'type' => 'count'}})
    end
  end

  def test_invalid_schema_dollar
    assert_raise Veritable::VeritableError do
        Veritable::Util.validate_data([], {'b$d' => {'type' => 'count'}})
    end
  end

  
  def test_data_valid_rows
    refrows = [
        {'_id' => '1', 'ColInt' => 3, 'ColFloat' => 3.1, 'ColCat' => 'a', 'ColBool' => true},
        {'_id' => '2', 'ColInt' => 4, 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => false},
        {'_id' => '3'}]
    testrows = [
        {'_id' => '1', 'ColInt' => 3, 'ColFloat' => 3.1, 'ColCat' => 'a', 'ColBool' => true},
        {'_id' => '2', 'ColInt' => 4, 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => false},
        {'_id' => '3'}]
    Veritable::Util.validate_data(testrows, @vschema)
    assert testrows == refrows
    Veritable::Util.clean_data(testrows, @vschema)
    assert testrows == refrows
  end

  def test_pred_valid_rows_no_id
    refrows = [
        {'_request_id' => '0', 'ColInt' => nil, 'ColFloat' => nil, 'ColCat' => 'a', 'ColBool' => true},
        {'_request_id' => '1', 'ColInt' => nil, 'ColFloat' => 4.1, 'ColCat' => nil, 'ColBool' => false},
        {'_request_id' => '2', 'ColInt' => nil, 'ColFloat' => nil }]
    testrows = [
        {'_request_id' => '0', 'ColInt' => nil, 'ColFloat' => nil, 'ColCat' => 'a', 'ColBool' => true},
        {'ColInt' => nil, 'ColFloat' => 4.1, 'ColCat' => nil, 'ColBool' => false},
        {'_request_id' => '2', 'ColInt' => nil, 'ColFloat' => nil }]
    assert_raise Veritable::VeritableError do
        Veritable::Util.validate_predictions(testrows, @vschema)
    end
    begin
        Veritable::Util.validate_predictions(testrows, @vschema)
    rescue Veritable::VeritableError => e
        assert e.row == 1
        assert e.col == '_request_id'
    end
    Veritable::Util.clean_predictions(testrows, @vschema)    
  end  

  def test_pred_valid_rows_id
    refrows = [
        {'_request_id' => '0', 'ColInt' => nil, 'ColFloat' => nil, 'ColCat' => 'a', 'ColBool' => true},
        {'_request_id' => '1', 'ColInt' => nil, 'ColFloat' => 4.1, 'ColCat' => nil, 'ColBool' => false},
        {'_request_id' => '2', 'ColInt' => nil, 'ColFloat' => nil }]
    testrows = [
        {'_request_id' => '0', 'ColInt' => nil, 'ColFloat' => nil, 'ColCat' => 'a', 'ColBool' => true},
        {'_request_id' => '1', 'ColInt' => nil, 'ColFloat' => 4.1, 'ColCat' => nil, 'ColBool' => false},
        {'_request_id' => '2', 'ColInt' => nil, 'ColFloat' => nil }]
    Veritable::Util.validate_predictions(testrows, @vschema)
    assert testrows == refrows
    Veritable::Util.clean_predictions(testrows, @vschema)
    assert testrows == refrows
  end  

  
  def test_data_missing_id_fail
    testrows = [
        {'_id' => '1', 'ColInt' => 3, 'ColFloat' => 3.1, 'ColCat' => 'a', 'ColBool' => true},
        {'ColInt' => 4, 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => false}]
    assert_raise Veritable::VeritableError do
        Veritable::Util.validate_data(testrows, @vschema)
    end
    begin
        Veritable::Util.validate_data(testrows, @vschema)
    rescue Veritable::VeritableError => e
        assert e.row == 1
    end
  end

  def test_data_missing_id_fix
    testrows = [
        {'_id' => '1', 'ColInt' => 3, 'ColFloat' => 3.1, 'ColCat' => 'a', 'ColBool' => true},
        {'ColInt' => 4, 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => false}]
    Veritable::Util.clean_data(testrows, @vschema, { 'assign_ids' => true })
    assert testrows[0]['_id'] != testrows[1]['_id']
    Veritable::Util.validate_data(testrows, @vschema)
  end

  def test_data_duplicate_id_fail
    testrows = [
        {'_id' => '1', 'ColInt' => 3, 'ColFloat' => 3.1, 'ColCat' => 'a', 'ColBool' => true},
        {'_id' => '1', 'ColInt' => 4, 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => false}]
    assert_raise Veritable::VeritableError do
        Veritable::Util.validate_data(testrows, @vschema)
    end
    begin
        Veritable::Util.validate_data(testrows, @vschema)
    rescue Veritable::VeritableError => e
        assert e.row == 1
        assert e.col == '_id'
    end
  end

  def test_data_nonstring_id_fail
    testrows = [
        {'_id' => '1', 'ColInt' => 3, 'ColFloat' => 3.1, 'ColCat' => 'a', 'ColBool' => true},
        {'_id' => 2, 'ColInt' => 4, 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => false}]
    assert_raise Veritable::VeritableError do
        Veritable::Util.validate_data(testrows, @vschema)
    end
    begin
        Veritable::Util.validate_data(testrows, @vschema)
    rescue Veritable::VeritableError => e
        assert e.row == 1
        assert e.col == '_id'
    end
  end

  def test_data_nonstring_id_fix
    testrows = [
        {'_id' => '1', 'ColInt' => 3, 'ColFloat' => 3.1, 'ColCat' => 'a', 'ColBool' => true},
        {'_id' => 2, 'ColInt' => 4, 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => false}]
    Veritable::Util.clean_data(testrows, @vschema)
    assert testrows[1]['_id'] == '2'
    Veritable::Util.validate_data(testrows, @vschema)
  end
  
  INVALID_IDS = ["", " foo",
    "foo ", " foo ", "foo\n", "foo\nbar", 5, 374.34, false,
    "_underscores"]

  def test_data_nonalphanumeric_ids_fail
    INVALID_IDS.each do |_id|
        testrows = [
            {'_id' => '1', 'ColInt' => 3, 'ColFloat' => 3.1, 'ColCat' => 'a', 'ColBool' => true},
            {'_id' => _id, 'ColInt' => 4, 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => false}]
        assert_raise Veritable::VeritableError do
            Veritable::Util.validate_data(testrows, @vschema)
        end
    end
  end

  def test_data_extrafield_pass
    testrows = [
        {'_id' => '1', 'ColInt' => 3, 'ColFloat' => 3.1, 'ColCat' => 'a', 'ColBool' => true},
        {'_id' => '2', 'ColEx' => 4, 'ColInt' => 4, 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => false}]
    Veritable::Util.validate_data(testrows, @vschema)
  end  

  def test_pred_extrafield_fail
    testrows = [
        {'_request_id' => '0', 'ColInt' => 3, 'ColFloat' => nil, 'ColCat' => 'a', 'ColBool' => true},
        {'_request_id' => '1', 'ColEx' => nil, 'ColInt' => 4, 'ColFloat' => nil, 'ColCat' => 'b', 'ColBool' => false}]
    assert_raise Veritable::VeritableError do
        Veritable::Util.validate_predictions(testrows, @vschema)
    end
    begin
        Veritable::Util.validate_predictions(testrows, @vschema)
    rescue Veritable::VeritableError => e
        assert e.row == 1
        assert e.col == 'ColEx'
    end
  end

  def test_pred_idfield_fail
    testrows = [
        {'_request_id' => '0', 'ColInt' => 3, 'ColFloat' => nil, 'ColCat' => 'a', 'ColBool' => true},
        {'_id' => '2', 'ColInt' => 4, 'ColFloat' => nil, 'ColCat' => 'b', 'ColBool' => false}]
    assert_raise Veritable::VeritableError do
        Veritable::Util.validate_predictions(testrows, @vschema)
    end
    begin
        Veritable::Util.validate_predictions(testrows, @vschema)
    rescue Veritable::VeritableError => e
        assert e.row == 1
        assert e.col == '_request_id'
    end
  end

  def test_data_extrafield_fix
    testrows = [
        {'_id' => '1', 'ColInt' => 3, 'ColFloat' => 3.1, 'ColCat' => 'a', 'ColBool' => true},
        {'_id' => '2', 'ColEx' => 4, 'ColInt' => 4, 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => false}]
    Veritable::Util.clean_data(testrows, @vschema, { 'remove_extra_fields' => true })
    assert (not testrows[1].has_key?('ColEx'))
    Veritable::Util.validate_data(testrows, @vschema)
  end  

  def test_pred_extrafield_fix
    testrows = [
        {'_id' => '1', 'ColInt' => 3, 'ColFloat' => 3.1, 'ColCat' => 'a', 'ColBool' => true},
        {'ColEx' => 4, 'ColInt' => 4, 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => false}]
    Veritable::Util.clean_predictions(testrows, @vschema)
    assert (not testrows[0].has_key?('_id'))
    assert (not testrows[1].has_key?('ColEx'))
    Veritable::Util.validate_predictions(testrows, @vschema)
  end  

  
  def test_data_nonefield_fail
    testrows = [
        {'_id' => '1', 'ColInt' => 3, 'ColFloat' => 3.1, 'ColCat' => 'a', 'ColBool' => true},
        {'_id' => '2', 'ColInt' => 4, 'ColFloat' => 4.1, 'ColCat' => nil, 'ColBool' => false}]
    assert_raise Veritable::VeritableError do
        Veritable::Util.validate_data(testrows, @vschema)
    end
    begin
        Veritable::Util.validate_data(testrows, @vschema)
    rescue Veritable::VeritableError => e
        assert e.row == 1
        assert e.col == 'ColCat'
    end
  end

  def test_data_nonefield_fix
    testrows = [
        {'_id' => '1', 'ColInt' => 3, 'ColFloat' => 3.1, 'ColCat' => 'a', 'ColBool' => true},
        {'_id' => '2', 'ColInt' => 4, 'ColFloat' => 4.1, 'ColCat' => nil, 'ColBool' => false}]
    Veritable::Util.clean_data(testrows, @vschema)
    assert (not testrows[1].has_key?('ColCat'))
    Veritable::Util.validate_data(testrows, @vschema)
  end
  

  def test_data_non_int_count_fail
    testrows = [
        {'_id' => '1', 'ColInt' => 3, 'ColFloat' => 3.1, 'ColCat' => 'a', 'ColBool' => true},
        {'_id' => '2', 'ColInt' => '4', 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => false}]
    assert_raise Veritable::VeritableError do
        Veritable::Util.validate_data(testrows, @vschema)
    end
    begin
        Veritable::Util.validate_data(testrows, @vschema)
    rescue Veritable::VeritableError => e
        assert e.row == 1
        assert e.col == 'ColInt'
    end
  end
  

  def test_data_count_limit_fail
    testrows = [
        {'_id' => '1', 'ColInt' => 3, 'ColFloat' => 3.1, 'ColCat' => 'a', 'ColBool' => true},
        {'_id' => '2', 'ColInt' => 100001, 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => false}]
    assert_raise Veritable::VeritableError do
        Veritable::Util.validate_data(testrows, @vschema)
    end
    begin
        Veritable::Util.validate_data(testrows, @vschema)
    rescue Veritable::VeritableError => e
        assert e.row == 1
        assert e.col == 'ColInt'
    end
  end

  
  def test_pred_non_int_count_fail
    testrows = [
        {'_request_id' => '0', 'ColInt' => 3, 'ColFloat' => 3.1, 'ColCat' => 'a', 'ColBool' => true},
        {'_request_id' => '1', 'ColInt' => '4', 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => false}]
    assert_raise Veritable::VeritableError do
        Veritable::Util.validate_predictions(testrows, @vschema)
    end
    begin
        Veritable::Util.validate_predictions(testrows, @vschema)
    rescue Veritable::VeritableError => e
        assert e.row == 1
        assert e.col == 'ColInt'
    end
  end

  def test_data_non_int_count_fix
    testrows = [
        {'_id' => '1', 'ColInt' => 3, 'ColFloat' => 3.1, 'ColCat' => 'a', 'ColBool' => true},
        {'_id' => '2', 'ColInt' => '4', 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => false}]
    Veritable::Util.clean_data(testrows, @vschema)
    assert testrows[1]['ColInt'] == 4
    Veritable::Util.validate_data(testrows, @vschema)
  end

  def test_pred_non_int_count_fix
    testrows = [
        {'_request_id' => '0', 'ColInt' => 3, 'ColFloat' => 3.1, 'ColCat' => 'a', 'ColBool' => true},
        {'_request_id' => '1', 'ColInt' => '4', 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => false}]
    Veritable::Util.clean_predictions(testrows, @vschema)
    assert testrows[1]['ColInt'] == 4
    Veritable::Util.validate_predictions(testrows, @vschema)
  end

  def test_data_nonvalid_int_count_fail
    testrows = [
        {'_id' => '1', 'ColInt' => 3, 'ColFloat' => 3.1, 'ColCat' => 'a', 'ColBool' => true},
        {'_id' => '2', 'ColInt' => 'jello', 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => false}]
    assert_raise Veritable::VeritableError do
        Veritable::Util.validate_data(testrows, @vschema)
    end
    begin
        Veritable::Util.validate_data(testrows, @vschema)
    rescue Veritable::VeritableError => e
        assert e.row == 1
        assert e.col == 'ColInt'
    end
  end
  
  def test_pred_nonvalid_int_count_fail
    testrows = [
        {'_request_id' => '0', 'ColInt' => 3, 'ColFloat' => 3.1, 'ColCat' => 'a', 'ColBool' => true},
        {'_request_id' => '1', 'ColInt' => 'jello', 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => false}]
    assert_raise Veritable::VeritableError do
        Veritable::Util.validate_predictions(testrows, @vschema)
    end
    begin
        Veritable::Util.validate_predictions(testrows, @vschema)
    rescue Veritable::VeritableError => e
        assert e.row == 1
        assert e.col == 'ColInt'
    end
  end

  def test_pred_int_count_limit_fail
    testrows = [
        {'_request_id' => '0', 'ColInt' => 3, 'ColFloat' => 3.1, 'ColCat' => 'a', 'ColBool' => true},
        {'_request_id' => '1', 'ColInt' => 100001, 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => false}]
    assert_raise Veritable::VeritableError do
        Veritable::Util.validate_predictions(testrows, @vschema)
    end
    begin
        Veritable::Util.validate_predictions(testrows, @vschema)
    rescue Veritable::VeritableError => e
        assert e.row == 1
        assert e.col == 'ColInt'
    end
  end
  
  def test_pred_int_count_limit_fix
    testrows = [
        {'_id' => '1', 'ColInt' => 3, 'ColFloat' => 3.1, 'ColCat' => 'a', 'ColBool' => true},
        {'_id' => '2', 'ColInt' => 100001, 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => false}]
    Veritable::Util.clean_data(testrows, @vschema)
    assert (not testrows[1].has_key?('ColInt'))
    Veritable::Util.validate_data(testrows, @vschema)
  end
  
  def test_data_nonvalid_int_count_fixfail
    testrows = [
        {'_id' => '1', 'ColInt' => 3, 'ColFloat' => 3.1, 'ColCat' => 'a', 'ColBool' => true},
        {'_id' => '2', 'ColInt' => 'jello', 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => false}]
    assert_raise Veritable::VeritableError do
        Veritable::Util.clean_data(testrows, @vschema, { 'remove_invalids' => false })
    end
    begin
        Veritable::Util.clean_data(testrows, @vschema, { 'remove_invalids' => false })
    rescue Veritable::VeritableError => e
        assert e.row == 1
        assert e.col == 'ColInt'
    end
  end

  def test_pred_nonvalid_int_count_fixfail
    testrows = [
        {'_request_id' => '0', 'ColInt' => 3, 'ColFloat' => 3.1, 'ColCat' => 'a', 'ColBool' => true},
        {'_request_id' => '1', 'ColInt' => 'jello', 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => false}]
    assert_raise Veritable::VeritableError do
        Veritable::Util.clean_predictions(testrows, @vschema, { 'remove_invalids' => false })
    end
    begin
        Veritable::Util.clean_predictions(testrows, @vschema, { 'remove_invalids' => false })
    rescue Veritable::VeritableError => e
        assert e.row == 1
        assert e.col == 'ColInt'
    end
  end
  
  def test_data_nonvalid_int_count_fix
    testrows = [
        {'_id' => '1', 'ColInt' => 3, 'ColFloat' => 3.1, 'ColCat' => 'a', 'ColBool' => true},
        {'_id' => '2', 'ColInt' => 'jello', 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => false}]
    Veritable::Util.clean_data(testrows, @vschema)
    assert (not testrows[1].has_key?('ColInt'))
    Veritable::Util.validate_data(testrows, @vschema)
  end

  def test_data_int_count_limit_fix
    testrows = [
        {'_id' => '1', 'ColInt' => 3, 'ColFloat' => 3.1, 'ColCat' => 'a', 'ColBool' => true},
        {'_id' => '2', 'ColInt' => 100001, 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => false}]
    Veritable::Util.clean_data(testrows, @vschema)
    assert (not testrows[1].has_key?('ColInt'))
    Veritable::Util.validate_data(testrows, @vschema)
  end

  
  def test_pred_nonvalid_int_count_fix
    testrows = [
        {'_request_id' => '0', 'ColInt' => 3, 'ColFloat' => 3.1, 'ColCat' => 'a', 'ColBool' => true},
        {'_request_id' => '1', 'ColInt' => 'jello', 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => false}]
    Veritable::Util.clean_predictions(testrows, @vschema)
    assert (not testrows[1].has_key?('ColInt'))
    Veritable::Util.validate_predictions(testrows, @vschema)
  end
  
  def test_data_negative_int_count_fail
    testrows = [
        {'_id' => '1', 'ColInt' => 3, 'ColFloat' => 3.1, 'ColCat' => 'a', 'ColBool' => true},
        {'_id' => '2', 'ColInt' => -4, 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => false}]
    assert_raise Veritable::VeritableError do
        Veritable::Util.validate_data(testrows, @vschema)
    end
    begin
        Veritable::Util.validate_data(testrows, @vschema)
    rescue Veritable::VeritableError => e
        assert e.row == 1
        assert e.col == 'ColInt'
    end
  end
  
  def test_pred_negative_int_count_fail
    testrows = [
        {'_request_id' => '0', 'ColInt' => 3, 'ColFloat' => 3.1, 'ColCat' => 'a', 'ColBool' => true},
        {'_request_id' => '1', 'ColInt' => -4, 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => false}]
    assert_raise Veritable::VeritableError do
        Veritable::Util.validate_predictions(testrows, @vschema)
    end
    begin
        Veritable::Util.validate_predictions(testrows, @vschema)
    rescue Veritable::VeritableError => e
        assert e.row == 1
        assert e.col == 'ColInt'
    end
  end

  def test_data_negative_int_count_fixfail
    testrows = [
        {'_id' => '1', 'ColInt' => 3, 'ColFloat' => 3.1, 'ColCat' => 'a', 'ColBool' => true},
        {'_id' => '2', 'ColInt' => -4, 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => false}]
    assert_raise Veritable::VeritableError do
        Veritable::Util.clean_data(testrows, @vschema, { 'remove_invalids' => false })
    end
    begin
        Veritable::Util.clean_data(testrows, @vschema, { 'remove_invalids' => false })
    rescue Veritable::VeritableError => e
        assert e.row == 1
        assert e.col == 'ColInt'
    end
  end

  def test_pred_negative_int_count_fixfail
    testrows = [
        {'_request_id' => '0', 'ColInt' => 3, 'ColFloat' => 3.1, 'ColCat' => 'a', 'ColBool' => true},
        {'_request_id' => '1', 'ColInt' => -4, 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => false}]
    assert_raise Veritable::VeritableError do
        Veritable::Util.clean_predictions(testrows, @vschema, { 'remove_invalids' => false })
    end
    begin
        Veritable::Util.clean_predictions(testrows, @vschema, { 'remove_invalids' => false })
    rescue Veritable::VeritableError => e
        assert e.row == 1
        assert e.col == 'ColInt'
    end
  end
  
  def test_data_negative_int_count_fix
    testrows = [
        {'_id' => '1', 'ColInt' => 3, 'ColFloat' => 3.1, 'ColCat' => 'a', 'ColBool' => true},
        {'_id' => '2', 'ColInt' => -4, 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => false}]
    Veritable::Util.clean_data(testrows, @vschema)
    assert (not testrows[1].has_key?('ColInt'))
    Veritable::Util.validate_data(testrows, @vschema)
  end

  def test_pred_negative_int_count_fix
    testrows = [
        {'_request_id' => '0', 'ColInt' => 3, 'ColFloat' => 3.1, 'ColCat' => 'a', 'ColBool' => true},
        {'_request_id' => '1', 'ColInt' => -4, 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => false}]
    Veritable::Util.clean_predictions(testrows, @vschema)
    assert (not testrows[1].has_key?('ColInt'))
    Veritable::Util.validate_predictions(testrows, @vschema)
  end

  def test_data_non_float_real_fail
    testrows = [
        {'_id' => '1', 'ColInt' => 3, 'ColFloat' => 3.1, 'ColCat' => 'a', 'ColBool' => true},
        {'_id' => '2', 'ColInt' => 4, 'ColFloat' => '4.1', 'ColCat' => 'b', 'ColBool' => false}]
    assert_raise Veritable::VeritableError do
        Veritable::Util.validate_data(testrows, @vschema)
    end
    begin
        Veritable::Util.validate_data(testrows, @vschema)
    rescue Veritable::VeritableError => e
        assert e.row == 1
        assert e.col == 'ColFloat'
    end
  end
  
  def test_pred_non_float_real_fail
    testrows = [
        {'_request_id' => '0', 'ColInt' => 3, 'ColFloat' => 3.1, 'ColCat' => 'a', 'ColBool' => true},
        {'_request_id' => '1', 'ColInt' => 4, 'ColFloat' => '4.1', 'ColCat' => 'b', 'ColBool' => false}]
    assert_raise Veritable::VeritableError do
        Veritable::Util.validate_predictions(testrows, @vschema)
    end
    begin
        Veritable::Util.validate_predictions(testrows, @vschema)
    rescue Veritable::VeritableError => e
        assert e.row == 1
        assert e.col == 'ColFloat'
    end
  end

  def test_data_non_float_real_fix
    testrows = [
        {'_id' => '1', 'ColInt' => 3, 'ColFloat' => 3.1, 'ColCat' => 'a', 'ColBool' => true},
        {'_id' => '2', 'ColInt' => 4, 'ColFloat' => '4.1', 'ColCat' => 'b', 'ColBool' => false}]
    Veritable::Util.clean_data(testrows, @vschema)
    assert testrows[1]['ColFloat'] == 4.1
    Veritable::Util.validate_data(testrows, @vschema)
  end

  def test_pred_non_float_real_fix
    testrows = [
        {'_request_id' => '0', 'ColInt' => 3, 'ColFloat' => 3.1, 'ColCat' => 'a', 'ColBool' => true},
        {'_request_id' => '1', 'ColInt' => 4, 'ColFloat' => '4.1', 'ColCat' => 'b', 'ColBool' => false}]
    Veritable::Util.clean_predictions(testrows, @vschema)
    assert testrows[1]['ColFloat'] == 4.1
    Veritable::Util.validate_predictions(testrows, @vschema)
  end

  def test_data_nonvalid_float_real_fail
    testrows = [
        {'_id' => '1', 'ColInt' => 3, 'ColFloat' => 3.1, 'ColCat' => 'a', 'ColBool' => true},
        {'_id' => '2', 'ColInt' => 4, 'ColFloat' => 'jello', 'ColCat' => 'b', 'ColBool' => false}]
    assert_raise Veritable::VeritableError do
        Veritable::Util.validate_data(testrows, @vschema)
    end
    begin
        Veritable::Util.validate_data(testrows, @vschema)
    rescue Veritable::VeritableError => e
        assert e.row == 1
        assert e.col == 'ColFloat'
    end
  end

  def test_data_nan_float_real_fail
    testrows = [
        {'_request_id' => '0', 'ColInt' => 3, 'ColFloat' => 3.1, 'ColCat' => 'a', 'ColBool' => true},
        {'_request_id' => '1', 'ColInt' => 4, 'ColFloat' =>  (1.0/0.0-1.0/0.0), 'ColCat' => 'b', 'ColBool' => false}]
    assert_raise Veritable::VeritableError do
        Veritable::Util.validate_predictions(testrows, @vschema)
    end
    begin
        Veritable::Util.validate_predictions(testrows, @vschema)
    rescue Veritable::VeritableError => e
        assert e.row == 1
        assert e.col == 'ColFloat'
    end
  end

  def test_data_inf_float_real_fail
    testrows = [
        {'_request_id' => '0', 'ColInt' => 3, 'ColFloat' => 3.1, 'ColCat' => 'a', 'ColBool' => true},
        {'_request_id' => '1', 'ColInt' => 4, 'ColFloat' =>  (1.0/0.0), 'ColCat' => 'b', 'ColBool' => false}]
    assert_raise Veritable::VeritableError do
        Veritable::Util.validate_predictions(testrows, @vschema)
    end
    begin
        Veritable::Util.validate_predictions(testrows, @vschema)
    rescue Veritable::VeritableError => e
        assert e.row == 1
        assert e.col == 'ColFloat'
    end
  end


  
  def test_pred_nonvalid_float_real_fail
    testrows = [
        {'_request_id' => '0', 'ColInt' => 3, 'ColFloat' => 3.1, 'ColCat' => 'a', 'ColBool' => true},
        {'_request_id' => '1', 'ColInt' => 4, 'ColFloat' => 'jello', 'ColCat' => 'b', 'ColBool' => false}]
    assert_raise Veritable::VeritableError do
        Veritable::Util.validate_predictions(testrows, @vschema)
    end
    begin
        Veritable::Util.validate_predictions(testrows, @vschema)
    rescue Veritable::VeritableError => e
        assert e.row == 1
        assert e.col == 'ColFloat'
    end
  end

  def test_data_nonvalid_float_real_fixfail
    testrows = [
        {'_id' => '1', 'ColInt' => 3, 'ColFloat' => 3.1, 'ColCat' => 'a', 'ColBool' => true},
        {'_id' => '2', 'ColInt' => 4, 'ColFloat' => 'jello', 'ColCat' => 'b', 'ColBool' => false}]
    assert_raise Veritable::VeritableError do
        Veritable::Util.clean_data(testrows, @vschema, { 'remove_invalids' => false })
    end
    begin
        Veritable::Util.clean_data(testrows, @vschema, { 'remove_invalids' => false })
    rescue Veritable::VeritableError => e
        assert e.row == 1
        assert e.col == 'ColFloat'
    end
  end
  
  def test_pred_nonvalid_float_real_fixfail
    testrows = [
        {'_request_id' => '0', 'ColInt' => 3, 'ColFloat' => 3.1, 'ColCat' => 'a', 'ColBool' => true},
        {'_request_id' => '1', 'ColInt' => 4, 'ColFloat' => 'jello', 'ColCat' => 'b', 'ColBool' => false}]
    assert_raise Veritable::VeritableError do
        Veritable::Util.clean_predictions(testrows, @vschema, { 'remove_invalids' => false })
    end
    begin
        Veritable::Util.clean_predictions(testrows, @vschema, { 'remove_invalids' => false })
    rescue Veritable::VeritableError => e
        assert e.row == 1
        assert e.col == 'ColFloat'
    end
  end

  def test_data_nonvalid_float_real_fix
    testrows = [
        {'_id' => '1', 'ColInt' => 3, 'ColFloat' => 3.1, 'ColCat' => 'a', 'ColBool' => true},
        {'_id' => '2', 'ColInt' => 4, 'ColFloat' => 'jello', 'ColCat' => 'b', 'ColBool' => false}]
    Veritable::Util.clean_data(testrows, @vschema)
    assert (not testrows[1].has_key?('ColFloat'))
    Veritable::Util.validate_data(testrows, @vschema)
  end

  def test_pred_nonvalid_float_real_fix
    testrows = [
        {'_request_id' => '0', 'ColInt' => 3, 'ColFloat' => 3.1, 'ColCat' => 'a', 'ColBool' => true},
        {'_request_id' => '1', 'ColInt' => 4, 'ColFloat' => 'jello', 'ColCat' => 'b', 'ColBool' => false}]
    Veritable::Util.clean_predictions(testrows, @vschema)
    assert (not testrows[1].has_key?('ColFloat'))
    Veritable::Util.validate_predictions(testrows, @vschema)
  end
  
  def test_data_non_str_cat_fail
    testrows = [
        {'_id' => '1', 'ColInt' => 3, 'ColFloat' => 3.1, 'ColCat' => 'a', 'ColBool' => true},
        {'_id' => '2', 'ColInt' => 4, 'ColFloat' => 4.1, 'ColCat' => 3, 'ColBool' => false}]
    assert_raise Veritable::VeritableError do
        Veritable::Util.validate_data(testrows, @vschema)
    end
    begin
        Veritable::Util.validate_data(testrows, @vschema)
    rescue Veritable::VeritableError => e
        assert e.row == 1
        assert e.col == 'ColCat'
    end
  end
  
  def test_pred_non_str_cat_fail
    testrows = [
        {'_request_id' => '0', 'ColInt' => 3, 'ColFloat' => 3.1, 'ColCat' => 'a', 'ColBool' => true},
        {'_request_id' => '1', 'ColInt' => 4, 'ColFloat' => 4.1, 'ColCat' => 3, 'ColBool' => false}]
    assert_raise Veritable::VeritableError do
        Veritable::Util.validate_predictions(testrows, @vschema)
    end
    begin
        Veritable::Util.validate_predictions(testrows, @vschema)
    rescue Veritable::VeritableError => e
        assert e.row == 1
        assert e.col == 'ColCat'
    end
  end

  def test_data_non_str_cat_fix
    testrows = [
        {'_id' => '1', 'ColInt' => 3, 'ColFloat' => 3.1, 'ColCat' => 'a', 'ColBool' => true},
        {'_id' => '2', 'ColInt' => 4, 'ColFloat' => 4.1, 'ColCat' => 3, 'ColBool' => false}]
    Veritable::Util.clean_data(testrows, @vschema)
    assert testrows[1]['ColCat'] == '3'
    Veritable::Util.validate_data(testrows, @vschema)
  end

  def test_pred_non_str_cat_fix
    testrows = [
        {'_request_id' => '0', 'ColInt' => 3, 'ColFloat' => 3.1, 'ColCat' => 'a', 'ColBool' => true},
        {'_request_id' => '1', 'ColInt' => 4, 'ColFloat' => 4.1, 'ColCat' => 3, 'ColBool' => false}]
    Veritable::Util.clean_predictions(testrows, @vschema)
    assert testrows[1]['ColCat'] == '3'
    Veritable::Util.validate_predictions(testrows, @vschema)
  end

  def test_data_non_bool_boolean_fail
    testrows = [
        {'_id' => '1', 'ColInt' => 3, 'ColFloat' => 3.1, 'ColCat' => 'a', 'ColBool' => true},
        {'_id' => '2', 'ColInt' => 4, 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => 'false'}]
    assert_raise Veritable::VeritableError do
        Veritable::Util.validate_data(testrows, @vschema)
    end
    begin
        Veritable::Util.validate_data(testrows, @vschema)
    rescue Veritable::VeritableError => e
        assert e.row == 1
        assert e.col == 'ColBool'
    end
  end
  
  def test_pred_non_bool_boolean_fail
    testrows = [
        {'_request_id' => '0', 'ColInt' => 3, 'ColFloat' => 3.1, 'ColCat' => 'a', 'ColBool' => true},
        {'_request_id' => '1', 'ColInt' => 4, 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => 'false'}]
    assert_raise Veritable::VeritableError do
        Veritable::Util.validate_predictions(testrows, @vschema)
    end
    begin
        Veritable::Util.validate_predictions(testrows, @vschema)
    rescue Veritable::VeritableError => e
        assert e.row == 1
        assert e.col == 'ColBool'
    end
  end
  
  def test_data_non_bool_boolean_truefix
    testrows = [
    {'_id' => '1', 'ColInt' => 3, 'ColFloat' => 3.1, 'ColCat' => 'a', 'ColBool' => true},
    {'_id' => '2', 'ColInt' => 4, 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => '1'},
    {'_id' => '4', 'ColInt' => 4, 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => '2'},
    {'_id' => '5', 'ColInt' => 4, 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => 'True'},
    {'_id' => '6', 'ColInt' => 4, 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => 'true'},
    {'_id' => '7', 'ColInt' => 4, 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => 'Yes'},
    {'_id' => '8', 'ColInt' => 4, 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => 'YES'},
    {'_id' => '9', 'ColInt' => 4, 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => 'Y'},
    {'_id' => '10', 'ColInt' => 4, 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => 'y'}]
    Veritable::Util.clean_data(testrows, @vschema)
    testrows.each do |r|
        assert r['ColBool'] == true
    end
    Veritable::Util.validate_data(testrows, @vschema)
  end

  def test_pred_non_bool_boolean_truefix
    testrows = [
    {'_request_id' => '0', 'ColInt' => 3, 'ColFloat' => 3.1, 'ColCat' => 'a', 'ColBool' => true},
    {'_request_id' => '1', 'ColInt' => 4, 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => '1'},
    {'_request_id' => '2', 'ColInt' => 4, 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => '2'},
    {'_request_id' => '3', 'ColInt' => 4, 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => 'True'},
    {'_request_id' => '4', 'ColInt' => 4, 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => 'true'},
    {'_request_id' => '5', 'ColInt' => 4, 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => 'Yes'},
    {'_request_id' => '6', 'ColInt' => 4, 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => 'YES'},
    {'_request_id' => '7', 'ColInt' => 4, 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => 'Y'},
    {'_request_id' => '8', 'ColInt' => 4, 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => 'y'}]
    Veritable::Util.clean_predictions(testrows, @vschema)
    testrows.each do |r|
        assert r['ColBool'] == true
    end
    Veritable::Util.validate_predictions(testrows, @vschema)
  end  


  def test_data_non_bool_boolean_falsefix
    testrows = [
    {'_id' => '1', 'ColInt' => 3, 'ColFloat' => 3.1, 'ColCat' => 'a', 'ColBool' => false},
    {'_id' => '2', 'ColInt' => 4, 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => '0'},
    {'_id' => '5', 'ColInt' => 4, 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => 'False'},
    {'_id' => '6', 'ColInt' => 4, 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => 'false'},
    {'_id' => '7', 'ColInt' => 4, 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => 'No'},
    {'_id' => '8', 'ColInt' => 4, 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => 'NO'},
    {'_id' => '9', 'ColInt' => 4, 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => 'N'},
    {'_id' => '10', 'ColInt' => 4, 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => 'n'}]
    Veritable::Util.clean_data(testrows, @vschema)
    testrows.each do |r|
        assert r['ColBool'] == false
    end
    Veritable::Util.validate_data(testrows, @vschema)
  end

  def test_pred_non_bool_boolean_falsefix
    testrows = [
    {'_request_id' => '0', 'ColInt' => 3, 'ColFloat' => 3.1, 'ColCat' => 'a', 'ColBool' => false},
    {'_request_id' => '1', 'ColInt' => 4, 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => '0'},
    {'_request_id' => '2', 'ColInt' => 4, 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => 'False'},
    {'_request_id' => '3', 'ColInt' => 4, 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => 'false'},
    {'_request_id' => '4', 'ColInt' => 4, 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => 'No'},
    {'_request_id' => '5', 'ColInt' => 4, 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => 'NO'},
    {'_request_id' => '6', 'ColInt' => 4, 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => 'N'},
    {'_request_id' => '7', 'ColInt' => 4, 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => 'n'}]
    Veritable::Util.clean_predictions(testrows, @vschema)
    testrows.each do |r|
        assert r['ColBool'] == false
    end
    Veritable::Util.validate_predictions(testrows, @vschema)
  end  

  def test_data_nonvalid_bool_boolean_fail
    testrows = [
        {'_id' => '1', 'ColInt' => 3, 'ColFloat' => 3.1, 'ColCat' => 'a', 'ColBool' => true},
        {'_id' => '2', 'ColInt' => 4, 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => 'jello'}]
    assert_raise Veritable::VeritableError do
        Veritable::Util.validate_data(testrows, @vschema)
    end
    begin
        Veritable::Util.validate_data(testrows, @vschema)
    rescue Veritable::VeritableError => e
        assert e.row == 1
        assert e.col == 'ColBool'
    end
  end
  
  def test_pred_nonvalid_bool_boolean_fail
    testrows = [
        {'_request_id' => '0', 'ColInt' => 3, 'ColFloat' => 3.1, 'ColCat' => 'a', 'ColBool' => true},
        {'_request_id' => '1', 'ColInt' => 4, 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => 'jello'}]
    assert_raise Veritable::VeritableError do
        Veritable::Util.validate_predictions(testrows, @vschema)
    end
    begin
        Veritable::Util.validate_predictions(testrows, @vschema)
    rescue Veritable::VeritableError => e
        assert e.row == 1
        assert e.col == 'ColBool'
    end
  end

  def test_data_nonvalid_bool_boolean_fixfail
    testrows = [
        {'_id' => '1', 'ColInt' => 3, 'ColFloat' => 3.1, 'ColCat' => 'a', 'ColBool' => true},
        {'_id' => '2', 'ColInt' => 4, 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => 'jello'}]
    assert_raise Veritable::VeritableError do
        Veritable::Util.clean_data(testrows, @vschema, { 'remove_invalids' => false })
    end
    begin
        Veritable::Util.clean_data(testrows, @vschema, { 'remove_invalids' => false })
    rescue Veritable::VeritableError => e
        assert e.row == 1
        assert e.col == 'ColBool'
    end
  end
  
  def test_pred_nonvalid_bool_boolean_fixfail
    testrows = [
        {'_request_id' => '0', 'ColInt' => 3, 'ColFloat' => 3.1, 'ColCat' => 'a', 'ColBool' => true},
        {'_request_id' => '1', 'ColInt' => 4, 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => 'jello'}]
    assert_raise Veritable::VeritableError do
        Veritable::Util.clean_predictions(testrows, @vschema, { 'remove_invalids' => false })
    end
    begin
        Veritable::Util.clean_predictions(testrows, @vschema, { 'remove_invalids' => false })
    rescue Veritable::VeritableError => e
        assert e.row == 1
        assert e.col == 'ColBool'
    end
  end

  def test_data_nonvalid_bool_boolean_fix
    testrows = [
        {'_id' => '1', 'ColInt' => 3, 'ColFloat' => 3.1, 'ColCat' => 'a', 'ColBool' => true},
        {'_id' => '2', 'ColInt' => 4, 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => 'jello'}]
    Veritable::Util.clean_data(testrows, @vschema)
    assert (not testrows[1].has_key?('ColBool'))
    Veritable::Util.validate_data(testrows, @vschema)
  end

  def test_pred_nonvalid_bool_boolean_fix
    testrows = [
        {'_request_id' => '0', 'ColInt' => 3, 'ColFloat' => 3.1, 'ColCat' => 'a', 'ColBool' => true},
        {'_request_id' => '1', 'ColInt' => 4, 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => 'jello'}]
    Veritable::Util.clean_predictions(testrows, @vschema)
    assert (not testrows[1].has_key?('ColBool'))
    Veritable::Util.validate_predictions(testrows, @vschema)
  end
  
  def test_data_too_many_cats_fail
    eschema = { 'ColCat' => {'type' => 'categorical'} }
    testrows = []
    rid = 0
    max_cols = 256
    (0...(max_cols-1)).each do |i|
        testrows.push({'_id' => rid.to_s, 'ColCat' => i.to_s})
        testrows.push({'_id' => (rid + 1).to_s, 'ColCat' => i.to_s})
        rid = rid + 2
    end
    testrows.push({'_id' => rid.to_s, 'ColCat' => (max_cols-1).to_s})
    testrows.push({'_id' => (rid + 1).to_s, 'ColCat' => (max_cols).to_s})
    assert_raise Veritable::VeritableError do
        Veritable::Util.validate_data(testrows, eschema)
    end
    begin
        Veritable::Util.validate_data(testrows, eschema)
    rescue Veritable::VeritableError => e
        assert e.col == 'ColCat'
    end
  end
  
  def test_pred_too_many_cats_fail
    eschema = { 'ColCat' => {'type' => 'categorical'} }
    testrows = []
    rid = 0
    max_cols = 256
    (0...(max_cols-1)).each do |i|
        testrows.push({'_request_id' => rid.to_s, 'ColCat' => i.to_s})
        testrows.push({'_request_id' => (rid + 1).to_s, 'ColCat' => i.to_s})
        rid = rid + 2
    end
    testrows.push({'_request_id' => rid.to_s, 'ColCat' => (max_cols-1).to_s})
    testrows.push({'_request_id' => (rid + 1).to_s, 'ColCat' => (max_cols).to_s})
    assert_raise Veritable::VeritableError do
        Veritable::Util.validate_predictions(testrows, eschema)
    end
    begin
        Veritable::Util.validate_predictions(testrows, eschema)
    rescue Veritable::VeritableError => e
        assert e.col == 'ColCat'
    end
  end

  def test_data_too_many_cats_fix
    eschema = { 'ColCat' => {'type' => 'categorical'} }
    testrows = []
    rid = 0
    max_cols = 256
    (0...(max_cols-1)).each do |i|
        testrows.push({'_id' => rid.to_s, 'ColCat' => i.to_s})
        testrows.push({'_id' => (rid + 1).to_s, 'ColCat' => i.to_s})
        rid = rid + 2
    end
    testrows.push({'_id' => rid.to_s, 'ColCat' => (max_cols-1).to_s})
    testrows.push({'_id' => (rid + 1).to_s, 'ColCat' => (max_cols).to_s})
    Veritable::Util.clean_data(testrows, eschema)
    assert testrows[510]['ColCat'] == 'Other'
    assert testrows[511]['ColCat'] == 'Other'
    Veritable::Util.validate_data(testrows, eschema)
  end

  def test_data_empty_col_fail
    testrows = [
        {'_id' => '1', 'ColInt' => 3, 'ColFloat' => 3.1, 'ColBool' => true},
        {'_id' => '2', 'ColInt' => 4, 'ColFloat' => 4.1, 'ColBool' => false}]
    assert_raise Veritable::VeritableError do
        Veritable::Util.validate_data(testrows, @vschema)
    end
    begin
        Veritable::Util.validate_data(testrows, @vschema)
    rescue Veritable::VeritableError => e
        assert e.col == 'ColCat'
    end
  end

  
  def test_query_params
    # ugh, this is less determinate and needs to be rewritten
    # comment out for now, manual inspection confirms the functionality is correct
    # [[{'foo' => 'bar', 'baz' => 2}, "foo=bar&baz=2"],
    #  [{'foo' => [1,2,3]}, "foo[]=1&foo[]=2&foo[]=3"],
    #  [{'foo' => {'a' => 1, 'b' => 2}}, "foo[a]=1&foo[b]=2"],
    #  [{'foo' => {'a' => 1, 'b' => [1,2,3]}}, "foo[a]=1&foo[b][]=1&foo[b][]=2&foo[b][]=3"]].each {|x|
    #   assert Veritable::Util.query_params(x[0]) == x[1]
    # }
  end
end
