require 'veritable/connection'
require 'veritable/errors'

module Veritable
  # Abstracts the structure of Veritable API resources
  #
  # Users should not include this module.
  module VeritableResource
  	include Connection

    private

    # Private method: retrieves the appropriate link field from the resource doc
    def link(name)
      @doc['links'][name]
    end
  end
end
