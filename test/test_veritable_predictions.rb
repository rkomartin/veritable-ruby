# encoding: utf-8

require 'multi_json'
require 'test/unit'
require 'veritable'

class VeritablePredictionsTest < Test::Unit::TestCase
  def setup
    @api = Veritable.connect
    @t = @api.create_table
    @t.batch_upload_rows [
      {'_id' => 'onebug', 'zim' => 'zam', 'wos' => 19.2},
      {'_id' => 'twobug', 'zim' => 'vim', 'wos' => 11.3},
      {'_id' => 'threebug', 'zim' => 'fop', 'wos' => 17.5},
      {'_id' => 'fourbug', 'zim' => 'zop', 'wos' => 10.3},
      {'_id' => 'fivebug', 'zim' => 'zam', 'wos' => 9.3},
      {'_id' => 'sixbug', 'zim' => 'zop', 'wos' => 18.9}
    ]
    @s1 = Veritable::Schema.new({'zim' => {'type' => 'categorical'}, 'wos' => {'type' => 'real'}})
    @a1 = @t.create_analysis @s1

    @t2 = @api.create_table
    @t2.batch_upload_rows [
      {'_id' => 'row1', 'cat' => 'a', 'ct' => 0, 'real' => 1.02394, 'bool' => true},
      {'_id' => 'row2', 'cat' => 'b', 'ct' => 0, 'real' => 0.92131, 'bool' => false},
      {'_id' => 'row3', 'cat' => 'c', 'ct' => 1, 'real' => 1.82812, 'bool' => true},
      {'_id' => 'row4', 'cat' => 'c', 'ct' => 1, 'real' => 0.81271, 'bool' => true},
      {'_id' => 'row5', 'cat' => 'd', 'ct' => 2, 'real' => 1.14561, 'bool' => false},
      {'_id' => 'row6', 'cat' => 'a', 'ct' => 5, 'real' => 1.03412, 'bool' => false}
    ]
    @s2 = Veritable::Schema.new({
      'cat' => {'type' => 'categorical'},
      'ct' => {'type' => 'count'},
      'real' => {'type' => 'real'},
      'bool' => {'type' => 'boolean'}
    })
    @a2 = @t2.create_analysis @s2
  end

  def test_make_prediction
    @a1.wait
    @a2.wait
    o = MultiJson.decode(MultiJson.encode({'cat' => 'b', 'ct' => 2, 'real' => 3.1, 'bool' => false}))
    r = MultiJson.decode(MultiJson.encode({'cat' => 'b', 'ct' => 2, 'real' => nil, 'bool' => false}))
    pr = @a2.predict r
    assert pr.is_a? Hash
    assert pr.is_a? Veritable::Prediction
    assert pr.uncertainty.is_a? Hash
    assert pr.schema.is_a? Veritable::Schema
    assert pr.distribution.is_a? Array
    assert pr.request.is_a? Hash
    pr.keys.each {|k|
      assert pr[k].is_a? o[k].class
      assert pr.uncertainty[k].is_a? Float
      assert pr[k] == o[k] or r[k].nil?
    }
    pr.distribution.each {|d| assert d.is_a? Hash}

    pr = @a2.predict Hash.new
    pr = @a2.predict ({'real' => 1, 'bool' => nil})

    assert_raise(VeritableError) {@a2.predict [
      {'cat' => 'b', 'ct' => 2, 'real' => nil, 'bool' => false},
      {'cat' => 'b', 'ct' => 2, 'real' => nil, 'bool' => false}
    ]}

    assert_raise(VeritableError) {@a2.predict(r, count=10000)}

    assert_raise(VeritableError) {@a1.predict r}

    @a2.delete
  end

  def teardown
    @t.delete
    @t2.delete
  end
end