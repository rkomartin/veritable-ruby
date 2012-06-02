# encoding: utf-8

require 'simplecov'

SimpleCov.start do
  add_filter "/test/"
end

require 'test/unit'
require 'veritable'

INVALIDS = ['éléphant', '374.34', 'ajfh/d/sfd@#$', 'きんぴらごぼう', '', ' foo', 'foo ', ' foo ', "foo\n", "foo\nbar", 3, 1.414, false, true, '_underscore', '-dash']
