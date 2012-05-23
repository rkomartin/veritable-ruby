class VeritableError < StandardError
  attr_reader :message
  def initialize(message)
  	@message = message
  end
  def to_s; message; end
  def inspect; message; end
end