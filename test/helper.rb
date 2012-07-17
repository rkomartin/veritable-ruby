# encoding: utf-8

require 'simplecov'

SimpleCov.start do
  add_filter "/test/"
end

require 'test/unit'
require 'veritable'

INVALIDS = ['éléphant', '374.34', 'ajfh/d/sfd@#$', 'きんぴらごぼう', '', ' foo', 'foo ', ' foo ', "foo\n", "foo\nbar", 3, 1.414, false, true, '_underscore', '-dash']

class Class
  def publicize_methods
    saved_private_instance_methods = self.private_instance_methods
    self.class_eval { public *saved_private_instance_methods }
    yield
    self.class_eval { private *saved_private_instance_methods }
  end
end

# This cursor is an Enumerator that ensures only one pass is made through the source data
class OnePassCursor
    include Enumerable
    def initialize(r)
      @r = r
      @has_run = false
    end
    def each
      if @has_run
        raise Exception.new "Can only enumerate once"
      end
      @has_run = true
      @r.each do |x|
        yield x
      end
    end
end