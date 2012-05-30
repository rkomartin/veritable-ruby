class VeritableError < StandardError
  attr_reader :message
  def initialize(message, opts=nil)
    @message = message
    if opts.is_a? Hash
      @opts = opts
      class << self
        @opts.keys.each {|k|
          self.send(:define_method, k.to_sym) {
            @opts[k]
          }
        }
      end
    end
  end
  def to_s; message; end
  def inspect; message; end
end