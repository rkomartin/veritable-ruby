# encoding: utf-8

require 'helper'

module Boolean; end
class TrueClass; include Boolean; end
class FalseClass; include Boolean; end

class VeritablePredictionsTest < Test::Unit::TestCase

  class << self
    def startup
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
    end
    def shutdown
        @t2.delete
    end
  end
  
  def setup
    @a2 = self.class.instance_variable_get :@a2
  end

  def teardown
  end

  def type_match(a, b)
    if [true,false].include? a
        return [true,false].include? b
    end
    return a.is_a? b.class
  end
  
  def check_preds(schema_ref, reqs, preds)
    preds = preds.to_a
    reqs = reqs.to_a
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
            assert(type_match(pr[k], schema_ref[k]), "\nRequest: #{req} \nPredictions: #{pr} \nSchemaRef: #{schema_ref} \nKey: #{k}")
            assert((pr[k] == req[k] or req[k].nil?), "\nRequest: #{req} \nPredictions: #{pr} \nSchemaRef: #{schema_ref} \nKey: #{k}")
            assert((pr.uncertainty[k].is_a? Float), "\nRequest: #{req} \nUncertainty: #{pr.uncertainty} \nSchemaRef: #{schema_ref} \nKey: #{k}")
        }
        assert pr.distribution.is_a? Array
        pr.distribution.each {|d| 
            assert d.is_a? Hash
            assert d.size == pr.size
            d.keys.each {|k|
                assert(type_match(d[k], schema_ref[k]), "\nRequest: #{req} \nDistribution: #{d} \nSchemaRef: #{schema_ref} \nKey: #{k}")
                assert((d[k] == req[k] or req[k].nil?), "\nRequest: #{req} \nDistribution: #{d} \nSchemaRef: #{schema_ref} \nKey: #{k}")
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
    schema_ref = MultiJson.decode(MultiJson.encode({'cat' => 'b', 'ct' => 2, 'real' => 3.1, 'bool' => false}))
    rr = (0...1).collect {|i| MultiJson.decode(MultiJson.encode({'_request_id' => i.to_s, 'cat' => 'b', 'ct' => 2, 'real' => nil, 'bool' => false}))}
    prs = @a2.batch_predict rr
    check_preds(schema_ref,rr,prs)
    rr = (0...10).collect {|i| MultiJson.decode(MultiJson.encode({'_request_id' => i.to_s, 'cat' => 'b', 'ct' => 2, 'real' => nil, 'bool' => false}))}
    prs = @a2.batch_predict rr
    check_preds(schema_ref,rr,prs)
    prs = @a2.batch_predict OnePassCursor.new(rr)
    check_preds(schema_ref,rr,prs)
  end

  def test_make_prediction_with_empty_row
    r = {}
    pr = @a2.predict r
  end

  def test_make_prediction_with_invalid_column_fails
    r = {'cat' => 'b', 'ct' => 2, 'real' => nil, 'jello' => false}
    assert_raise(Veritable::VeritableError) {(@a2.predict r).to_a}
  end

  def test_make_batch_prediction_missing_request_id_fails
    rr = (0...2).collect {|i| MultiJson.decode(MultiJson.encode({'cat' => 'b', 'ct' => 2, 'real' => nil, 'bool' => false}))}
    assert_raise(Veritable::VeritableError) {(@a2.batch_predict rr).to_a}    
  end

  def test_batch_prediction_batching
    schema_ref = {'cat' => 'b', 'ct' => 2, 'real' => 3.1, 'bool' => false}
    rr = [ 
        {'_request_id' => 'a', 'cat' => nil, 'ct' => 2, 'real' => 3.1, 'bool' => false},
        {'_request_id' => 'b', 'cat' => 'b', 'ct' => nil, 'real' => 3.1, 'bool' => false},
        {'_request_id' => 'c', 'cat' => 'b', 'ct' => 2, 'real' => nil, 'bool' => false},
        {'_request_id' => 'd', 'cat' => 'b', 'ct' => 2, 'real' => 3.1, 'bool' => nil}
        ]
    @a2.class.publicize_methods do
        prs = @a2.raw_predict(rr.each,count=10,maxcells=30,maxcols=4)
        check_preds(schema_ref,rr,prs)
        prs = @a2.raw_predict(rr.each,count=10,maxcells=20,maxcols=4)
        check_preds(schema_ref,rr,prs)
        prs = @a2.raw_predict(rr.each,count=10,maxcells=17,maxcols=4)
        check_preds(schema_ref,rr,prs)
        prs = @a2.raw_predict(rr.each,count=10,maxcells=10,maxcols=4)
        check_preds(schema_ref,rr,prs)
    end
  end
  
  def test_batch_prediction_streaming
    schema_ref = {'cat' => 'b', 'ct' => 2, 'real' => 3.1, 'bool' => false}
    rr = [ 
        {'_request_id' => 'a', 'cat' => nil, 'ct' => 2, 'real' => 3.1, 'bool' => false},
        {'_request_id' => 'b', 'cat' => 'b', 'ct' => nil, 'real' => 3.1, 'bool' => false},
        {'_request_id' => 'c', 'cat' => 'b', 'ct' => 2, 'real' => nil, 'bool' => false},
        {'_request_id' => 'd', 'cat' => 'b', 'ct' => 2, 'real' => 3.1, 'bool' => nil}
        ]
    wrr = rr.each
    @a2.class.publicize_methods do
        prs = @a2.raw_predict(wrr,count=10,maxcells=1,maxcols=4)
        check_preds(schema_ref,rr,prs)
    end
  end
  
  def test_batch_prediction_count_batching
    schema_ref = {'cat' => 'b', 'ct' => 2, 'real' => 3.1, 'bool' => false}
    rr = [ 
        {'_request_id' => 'a', 'cat' => nil, 'ct' => 2, 'real' => 3.1, 'bool' => false},
        {'_request_id' => 'b', 'cat' => 'b', 'ct' => nil, 'real' => 3.1, 'bool' => false},
        {'_request_id' => 'c', 'cat' => 'b', 'ct' => 2, 'real' => nil, 'bool' => false},
        {'_request_id' => 'd', 'cat' => 'b', 'ct' => 2, 'real' => 3.1, 'bool' => nil}
        ].each
    @a2.class.publicize_methods do
        prs = @a2.raw_predict(rr,count=10,maxcells=1,maxcols=4)
        check_preds(schema_ref,rr,prs)
    end
  end
  
  def test_batch_prediction_too_many_cells
    schema_ref = {'cat' => 'b', 'ct' => 2, 'real' => 3.1, 'bool' => false}
    rr = [ {'_request_id' => 'a', 'cat' => nil, 'ct' => nil, 'real' => 3.1, 'bool' => false} ] 
    @a2.class.publicize_methods do
        prs = @a2.raw_predict(rr.each,count=10,maxcells=20,maxcols=4)
        check_preds(schema_ref,rr,prs)
        assert_raise(Veritable::VeritableError) {@a2.raw_predict(rr.each,count=10,maxcells=20,maxcols=3).to_a}
        assert_raise(Veritable::VeritableError) {@a2.raw_predict(rr.each,count=10,maxcells=1,maxcols=4).to_a}
    end
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
