require 'veritable/connection'
require 'veritable/errors'

module Veritable
  module VeritableResource
  	include Connection

    private

    def link(name)
      raise VeritableError unless @doc['links'].has_key?(name)
      @doc['links'][name]
    end
  end
end
