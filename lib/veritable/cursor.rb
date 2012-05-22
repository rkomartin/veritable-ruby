require 'veritable/object'

module Veritable
  class Cursor < VeritableResource
    include Enumerator

    def initialize(opts=nil, doc=nil)
      super(opts, doc)

      require_opts ['collection']
      default_opts({'per_page' => 100})

      collection_key = collection.split("/")[-1]
      @doc = get(collection, params={:per_page => per_page, :start => start})
      @doc.has_key?(collection_key) ? @opts['key'] = collection_key : @opts['key'] = 'data'
    end

    def inspect; to_s; end
    def to_s; "#<Veritable::Cursor collection='" + collection + "'>"; end

    private

    def limit; @opts['limit']; end
    def start; @opts['start']; end
    def per_page; @opts['per_page']; end
    def collection; @opts['collection'] end
    def key; @opts['key'] end
    def next_page; link 'next' end
    def last_page?; @doc.has_key? 'next' end
    def data; @doc[key] end
  end
end
