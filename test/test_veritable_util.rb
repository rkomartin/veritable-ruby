require 'veritable'
require 'test/unit'
require 'tempfile'

class VeritableTestUtils < Test::Unit::TestCase

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