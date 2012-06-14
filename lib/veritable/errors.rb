module Veritable

  # Class for all errors returned by veritable-ruby
  #
  # ==== Attributes
  # * +message+ -- the String message describing the error
  # * dynamically defined -- errors may have other attributes, such as +http_code+ or +row+, dynamically defined at initialization.
  class VeritableError < StandardError
    # Accessor for the error message
    attr_reader :message

    # Initializes a Veritable::VeritableError
    #
    # Users should not invoke directly.
    #
    # ==== Arguments
    # +message+ -- a String message describing the error
    # +opts+ -- a Hash optionally specifying other instance attributes to be dynamically defined
    #
    # 
    # See also: https://dev.priorknowledge.com/docs/client/ruby  
    def initialize(message, opts=nil)
      super(message)
      @message = message
      if opts.is_a? Hash
        @opts = opts
        eigenclass = class << self; self; end
        @opts.keys.each {|k|
          eigenclass.send(:define_method, k.to_sym) {
            @opts[k]
          }
        }
        if @opts.include? 'inner_error'
            set_backtrace(@opts['inner_error'].backtrace)
        end
      end
    end

    # Prints the error message
    def to_s; message; end

    # Prints the error message
    def inspect; message; end
  end
end