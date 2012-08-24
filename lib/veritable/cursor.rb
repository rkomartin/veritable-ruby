require 'veritable/object'
require 'veritable/resource'

module Veritable

  # Generic Cursor class for collections of API resources
  #
  # Cursors may be initialized with 'limit', 'start', and 'per_page' options.
  #
  # Users should call the #each and #next methods for access to the underlying resources.
  class Cursor
    include VeritableResource
    include Enumerable

    # Initializes a new Veritable::Cursor from an API collection
    #
    # Optionally pass a block in for postprocessing of resources.
    def initialize(opts=nil, doc=nil, &lazymap)
      super(opts, doc)

      require_opts 'collection'
      default_opts({'per_page' => 100})

      collection_key = collection.split("/")[-1]
      get_params = {:count => per_page, :start => start}
      get_params.update(@opts['extra_args']) if @opts['extra_args']
      @doc = get(collection, params=get_params)
      @doc.has_key?(collection_key) ? @opts['key'] = collection_key : @opts['key'] = 'data'
      @opts['lazymap'] = lazymap if lazymap
    end

    # Implements the Enumerable interface
    def each
      i = limit if limit
      loop do
        if data.length > 0 or refresh > 0
          if limit
            raise StopIteration if i == 0
            i = i - 1
          end
          if lazymap
            yield lazymap.call(data.shift)
          else
            yield data.shift
          end
        else
          raise StopIteration
        end
      end
    end

    # String representation of the Cursor
    def inspect; to_s; end

    # String representation of the Cursor
    def to_s; "#<Veritable::Cursor collection='" + collection + "'>"; end

    private

    # Private method to refresh the cursor from the server
    def refresh
      return data.length if data.length > 0
      if next_page
        @doc = get next_page
      elsif last_page?
        return 0
      else
        get_params = {:count => per_page, :start => start}
        get_params.update(@opts['extra_args']) if @opts['extra_args']
        @doc = get(collection, params={:count => per_page, :start => start})
      end
      return data.length
    end

    # Private accessor for the limit option
    def limit; @opts['limit']; end

    # Private setter for the limit option
    def limit=(x); @opts['limit'] = x; end

    # Private accessor for the start option
    def start; @opts['start']; end

    # Private accessor for the per_page option
    def per_page; @opts['per_page']; end

    # Privatre accessor for the collection
    def collection; @opts['collection'] end

    # Postprocessing block, if any
    def lazymap; @opts['lazymap']; end

    # Key for the underlying data
    def key; @opts['key'] end

    # Link to the next page of the collection
    def next_page; link 'next' end

    # True if the Cursor is on the last page of the collection
    def last_page?; ! @doc.has_key? 'next' end

    # Fetches the underlying data
    def data; @doc[key] end
  end
end
