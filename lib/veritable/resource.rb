require 'veritable/connection'
require 'veritable/errors'

module Veritable
  module VeritableResource
  	include Connection

    private

    def link(name)
      @doc['links'][name]
    end
  end
end
