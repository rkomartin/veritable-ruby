require 'veritable/errors'

module Veritable
  # Abstracts the structure of veritable-ruby objects
  #
  # Users should not include this module.
  module VeritableObject
    # Initializes a new object from a hash of options and an API doc
    #
    # Users should not invoke directly.
    def initialize(opts=nil, doc=nil)
      @opts = opts
      @doc = doc
    end

    private

    # Private method -- requires that certain options be present at initialization
    def require_opts(*keys)
      keys.each {|k| raise VeritableError.new("Error initializing object -- must provide #{k}") unless @opts.has_key?(k)}
    end

    # Private method -- specifies default options
    def default_opts(hash={})
      hash.each {|k, v| @opts[k] = v unless @opts.has_key?(k)}
    end
  end
end
