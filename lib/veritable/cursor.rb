require 'veritable/object'
require 'veritable/resource'

module Veritable
  class Cursor < Enumerator
    include VeritableResource

    def initialize(opts=nil, doc=nil)
      super(opts, doc)

      require_opts ['collection']
      default_opts({'per_page' => 100})

      collection_key = collection.split("/")[-1]
      @doc = get(collection, params={:count => per_page, :start => start})
      @doc.has_key?(collection_key) ? @opts['key'] = collection_key : @opts['key'] = 'data'
    end

    def next
      if data.length > 0 or refresh > 0
        if limit
          if limit == 0
            raise StopIteration
          end
          limit = limit - 1
        end
        return data.shift
      else
        raise StopIteration
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
    def key; @opts['key'] end
    def next_page; link 'next' end
    def last_page?; @doc.has_key? 'next' end
    def data; @doc[key] end
  end
end
