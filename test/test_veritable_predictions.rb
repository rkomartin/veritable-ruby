# encoding: utf-8

require 'helper'

module Boolean; end
class TrueClass; include Boolean; end
class FalseClass; include Boolean; end


class Class
  def publicize_methods
    saved_private_instance_methods = self.private_instance_methods
    self.class_eval { public *saved_private_instance_methods }
    yield
    self.class_eval { private *saved_private_instance_methods }
  end
end

class VeritablePredictionsTest < Test::Unit::TestCase
  def initialize(*args)
    @api = Veritable.connect

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
    @a2.wait
    super(*args)
  end

  def teardown
    @t2.delete
  end

  def check_preds(schema_ref, reqs, preds)
	assert reqs.size == preds.size
	(0...reqs.size).each {|i|
		req = reqs[i]
		pr = preds[i]
		assert pr.is_a? Hash
		assert pr.is_a? Veritable::Prediction
		assert pr.uncertainty.is_a? Hash
		if req.include? '_request_id'
			assert req['_request_id'] == pr.request_id
			assert req.size == (pr.size + 1)
		else
			assert pr.request_id.nil?
			assert req.size == pr.size
		end
		pr.keys.each {|k|
			assert pr[k].is_a? schema_ref[k].class
			assert (pr[k] == req[k] or req[k].nil?)
			assert pr.uncertainty[k].is_a? Float
		}
		assert pr.distribution.is_a? Array
		pr.distribution.each {|d| 
			assert d.is_a? Hash
			assert d.size == pr.size
			d.keys.each {|k|
				assert d[k].is_a? schema_ref[k].class
				assert (d[k] == req[k] or req[k].nil?)
			}
		}
	}
  end  
  
  def test_make_prediction
    schema_ref = MultiJson.decode(MultiJson.encode({'cat' => 'b', 'ct' => 2, 'real' => 3.1, 'bool' => false}))

    r = MultiJson.decode(MultiJson.encode({'cat' => 'b', 'ct' => 2, 'real' => nil, 'bool' => false}))
    pr = @a2.predict r
	check_preds(schema_ref, [r], [pr])

    r = MultiJson.decode(MultiJson.encode({'_request_id' => 'foo', 'cat' => 'b', 'ct' => 2, 'real' => nil, 'bool' => false}))
    pr = @a2.predict r
	check_preds(schema_ref, [r], [pr])
	
  end

  def test_make_batch_prediction
  end

  def test_make_prediction_with_empty_row
	r = {}
    pr = @a2.predict r
  end

  def test_make_prediction_with_invalid_column_fails
	r = {'cat' => 'b', 'ct' => 2, 'real' => nil, 'jello' => false}
	assert_raise(Veritable::VeritableError) {@a2.predict r}
  end

  def test_make_prediction_missing_request_id_fails
  end

  def test_batch_prediction_batching
	@a2.class.publicize_methods do
		@a2.raw_predict(nil,nil,nil,nil)
	end
  end
  
  def test_batch_prediction_too_many_cells
  end
  
  def test_make_predictions_with_fixed_int_val_for_float_col
	r = {'real' => 1, 'bool' => nil}
    pr = @a2.predict r
  end
  
  
end

class VeritablePredictionClassTest < Test::Unit::TestCase
  def setup
    request = {'ColInt' => nil, 'ColFloat' => nil, 'ColCat' => nil, 'ColBool' => nil}
    schema = {'ColInt' => {'type' => 'count'}, 'ColFloat' => {'type' => 'real'},
      'ColCat' => {'type' => 'categorical'}, 'ColBool' => {'type' => 'boolean'}}
    distribution = [
      {'ColInt' => 3, 'ColFloat' => 3.1, 'ColCat' => 'a', 'ColBool' => false},
      {'ColInt' => 4, 'ColFloat' => 4.1, 'ColCat' => 'b', 'ColBool' => false},
      {'ColInt' => 8, 'ColFloat' => 8.1, 'ColCat' => 'b', 'ColBool' => false},
      {'ColInt' => 11, 'ColFloat' => 2.1, 'ColCat' => 'c', 'ColBool' => true}
    ]
    @testpreds = Veritable::Prediction.new request, distribution, schema, nil
    @testpreds2 = Veritable::Prediction.new MultiJson.decode(MultiJson.encode(request)), MultiJson.decode(MultiJson.encode(distribution)), MultiJson.decode(MultiJson.encode(schema)), nil
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
        assert(expected.is_a?(v[0]), "Failed on #{k}, expected value is of class #{expected.class}, not #{v[0]}")
        if v[1] == :equal
          assert(expected == v[2], "Failed on #{k}, expected value is #{expected}, not equal to #{v[2]}")
        elsif v[1] == :approximate
          assert((expected - v[2]).abs < tolerance, "Failed on #{k}, expected value is #{expected}, not within #{tolerance} of #{v[2]}")
        else
          raise Exception.new
        end
        assert((uncertainty - v[3]).abs < tolerance, "Failed on #{k}, uncertainty is #{uncertainty}, not within #{tolerance} of #{v[3]}")
        p_within = tp.prob_within(k, v[4])
        assert((p_within - v[5]).abs < tolerance, "Failed on #{k}, probability within #{v[4]} is #{p_within}, not within #{tolerance} of #{v[5]}")
        c_values = tp.credible_values(k)
        assert(c_values == v[6], "Failed on #{k}, credible values are not equal to #{v[6]}")
        c_values = tp.credible_values(k, p=v[7])
        assert(c_values = v[8], "Failed on #{k}, credible values within #{v[7]} are not equal to #{v[8]}")
      }
    }
  end
end

class TestVeritableRelated < Test::Unit::TestCase
  def setup
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
  end

  def test_related_to
    @a.wait
    @schema.keys.each {|col|
      assert @a.related_to(col).to_a.size <= 5
    }
    assert_raise(Veritable::VeritableError) {@a.related_to('missing_col')}
    assert @a.related_to('cat', {'start' => 'real'}).to_a.size <= 5
    assert @a.related_to('cat', {'limit' => 0}).to_a.size == 0
    assert @a.related_to('cat', {'limit' => 3}).to_a.size <= 3
    assert @a.related_to('cat', {'limit' => 3}).to_a.size <= 5
  end

  def teardown
    @t.delete
  end
end

