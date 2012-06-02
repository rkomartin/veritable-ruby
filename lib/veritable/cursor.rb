require 'veritable/object'
require 'veritable/resource'

module Veritable
  class Cursor
    include VeritableResource
    include Enumerable
    def initialize(opts=nil, doc=nil, &lazymap)
      super(opts, doc)

      require_opts 'collection'
      default_opts({'per_page' => 100})

      collection_key = collection.split("/")[-1]
      @doc = get(collection, params={:count => per_page, :start => start})
      @doc.has_key?(collection_key) ? @opts['key'] = collection_key : @opts['key'] = 'data'
      @opts['lazymap'] = lazymap if lazymap
    end

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
    def inspect; to_s; end
    def to_s; "#<Veritable::Cursor collection='" + collection + "'>"; end

    private

    def refresh
      return data.length if data.length > 0
      if next_page
        @doc = get next_page
      elsif last_page?
        return 0
      else
        @doc = get(collection, params={:count => per_page, :start => start})
      end
      return data.length
    end

    def limit; @opts['limit']; end
    def limit=(x); @opts['limit'] = x; end
    def start; @opts['start']; end
    def per_page; @opts['per_page']; end
    def collection; @opts['collection'] end
    def lazymap; @opts['lazymap']; end
    def key; @opts['key'] end
    def next_page; link 'next' end
    def last_page?; ! @doc.has_key? 'next' end
    def data; @doc[key] end
  end
end
