require 'veritable'
require 'test/unit'

class VeritableTestUtils < Test::Unit::TestCase
  def test_query_params
    [[{'foo' => 'bar', 'baz' => 2}, "foo=bar&baz=2"],
     [{'foo' => [1,2,3]}, "foo[]=1&foo[]=2&foo[]=3"],
     [{'foo' => {'a' => 1, 'b' => 2}}, "foo[a]=1&foo[b]=2"],
     [{'foo' => {'a' => 1, 'b' => [1,2,3]}}, "foo[a]=1&foo[b][]=1&foo[b][]=2&foo[b][]=3"]].each {|x|
     	puts x[0]
     	puts x[1]
     	puts Veritable::Util.query_params(x[0])
      assert Veritable::Util.query_params(x[0]) == x[1]
    }
  end
end