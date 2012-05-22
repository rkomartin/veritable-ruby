require 'veritable/errors'

module Veritable
  module VeritableObject
    def initialize(opts=nil, doc=nil)
      @opts = opts
      @doc = doc
    end

    private

    def require_opts(keys)
      keys.each {|k| raise VeritableError unless @opts.has_key?(k)}
    end

    def default_opts(hash)
      hash.each {|k, v| @opts[k] = v unless @opts.has_key?(k)}
    end
  end
end
