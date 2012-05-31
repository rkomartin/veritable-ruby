# encoding: utf-8

require 'multi_json'
require 'test/unit'
require 'veritable'

module Boolean; end
class TrueClass; include Boolean; end
class FalseClass; include Boolean; end

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

class VeritablePredictionClassTest < Test::Unit::TestCase
  def setup
    @request = {'ColInt' => nil, 'ColFloat' => nil, 'ColCat' => nil, 'ColBool' => nil}
    @schema = {'ColInt' => {'type' => 'count'}, 'ColFloat' => {'type' => 'real'},
      'ColCat' => {'type' => 'categorical'}, 'ColBool' => {'type' => 'boolean'}}
    @distribution = [
      {'ColInt' => 3, 'ColFloat' => 3.1, 'ColCat' => 'a', 'ColBool' => false},
      {'ColInt' => 4, 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => false},
      {'ColInt' => 8, 'ColFloat' => 8.1, 'ColCat' => 'b', 'ColBool' => false},
      {'ColInt' => 11, 'ColFloat' => 2.1, 'ColCat' => 'c', 'ColBool' => true}
    ]
    @testpreds = Veritable::Prediction.new request, distribution, schema
    @testpreds2 = Veritable::Prediction.new MultiJson.decode(MultiJson.encode(request)), MultiJson.decode(MultiJson.encode(distribution)), MultiJson.decode(MultiJson.encode(schema))
  end

  def test_prediction_class
    tolerance = 0.001

    [@testpreds, @testpreds2].each {|tp|
      {'ColInt' => [Fixnum, :equal, ((3 + 4 + 8 + 11) / 4.0).round.to_i, 8, [5, 9], 0.25, [3, 11], 0.60, [4, 8]],
       'ColFloat' => [Float, :approximate, 4.35, 6, [5,9], 0.25, [2.1, 8.1], 0.60, [3.1, 4.1]],
       'ColCat' => [String, :equal, 'b', 0.5, ['b', 'c'], 0.75, {'b' => 0.5}, 0.10, {'a' => 0.25, 'b' => 0.5, 'c' => 0.25}],
       'ColBool' => [Boolean, :equal, false, 0.25, [true], 0.25, {false => 0.75}, 0.10, {true => 0.25, false => 0.75}]
      }.each {|k, v|
        expected = tp[k]
        uncertainty = tp.uncertainty[k]
        assert expected.is_a? v[0]
        if v[1] == :equal
          assert expected == v[2]
        elsif v[1] == :approximate
          assert (expected - v[2]).abs < tolerance
        else
          raise Exception
        end
        p_within = tp.prob_within(k, v[3])
        assert (p_within - v[4]).abs < tolerance
        c_values = tp.credible_values(k)
        assert c_values == v[5]
        c_values = tp.credible_values(k, p=v[6])
        assert c_values = v[7]
      }
    }
  end
end
