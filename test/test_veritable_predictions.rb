# encoding: utf-8

require 'multi_json'
require 'test/unit'
require 'veritable'

class VeritableTableOpTest < Test::Unit::TestCase
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
    @s1 = Schema.new({'zim': {'type': 'categorical'}, 'wos': {'type': 'real'}})
    @a1 = @t.create_analysis @s1

    @t2 = @api.create_table
    @t2.batch_upload_rows [
      {'_id' => 'row1', 'cat' => 'a', 'ct' => 0, 'real' => 1.02394, 'bool' => True},
      {'_id' => 'row2', 'cat' => 'b', 'ct' => 0, 'real' => 0.92131, 'bool' => False},
      {'_id' => 'row3', 'cat' => 'c', 'ct' => 1, 'real' => 1.82812, 'bool' => True},
      {'_id' => 'row4', 'cat' => 'c', 'ct' => 1, 'real' => 0.81271, 'bool' => True},
      {'_id' => 'row5', 'cat' => 'd', 'ct' => 2, 'real' => 1.14561, 'bool' => False},
      {'_id' => 'row6', 'cat' => 'a', 'ct' => 5, 'real' => 1.03412, 'bool' => False}
    ]
    @s2 = Schema.new({
      'cat': {'type': 'categorical'},
      'ct': {'type': 'count'},
      'real': {'type': 'real'},
      'bool': {'type': 'boolean'}
    })
    @a2 = @t2.create_analysis @s2
  end

  def teardown
    @t.delete
    @t2.delete
  end
end