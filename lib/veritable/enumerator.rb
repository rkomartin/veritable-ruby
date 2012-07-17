# https://github.com/marcandre/backports/blob/master/lib/backports/1.8.7/enumerator.rb
# https://github.com/marcandre/backports/tree/master/lib/backports/1.9.1/enumerator.rb
# https://github.com/marcandre/backports/blob/master/lib/backports/tools.rb

module Veritable
  require 'enumerator'
  if (::Enumerable::Enumerator rescue false)
    module Enumerable
      include ::Enumerable
      class Enumerator
        # Standard in Ruby 1.8.7+. See official documentation[http://ruby-doc.org/core-1.9/classes/Enumerator.html]
        Veritable.make_block_optional self, :each, :test_on => [42].to_enum

        def next
          require 'generator'
          @generator ||= Generator.new(self)
          raise StopIteration unless @generator.next?
          @generator.next
        end unless method_defined? :next

        def rewind
          @object.rewind if @object.respond_to? :rewind
          require 'generator'
          @generator ||= Generator.new(self)
          @generator.rewind
          self
        end unless method_defined? :rewind
      end if const_defined? :Enumerator
    end
    # Must be defined outside of Kernel for jruby, see http://jira.codehaus.org/browse/JRUBY-3609
    Enumerator = Veritable::Enumerable::Enumerator
  else
    Enumerator = ::Enumerable::Enumerator unless Object.const_defined? :Enumerator # Standard in ruby 1.9
  end

  class Enumerator
    # new with block, standard in Ruby 1.9
    unless (self.new{} rescue false)
        # A simple class which allows the construction of Enumerator from a block
      class Yielder
        def initialize(&block)
          @main_block = block
        end

        def each(&block)
          @final_block = block
          @main_block.call(self)
        end

        def yield(*arg)
          @final_block.yield(*arg)
        end

        def <<(*arg)
          @final_block.yield(*arg)
          self
        end
      end

      def initialize_with_optional_block(*arg, &block)
        return initialize_without_optional_block(*arg, &nil) unless arg.empty?  # Ruby 1.9 apparently ignores the block if any argument is present
        initialize_without_optional_block(Yielder.new(&block))
      end
      Veritable.alias_method_chain self, :initialize, :optional_block
    end
  end

  # Metaprogramming utility to make block optional.
  # Tests first if block is already optional when given options
  def self.make_block_optional(mod, *methods)
    options = methods.last.is_a?(Hash) ? methods.pop : {}
    methods.each do |selector|
      unless mod.method_defined? selector
        warn "#{mod}##{selector} is not defined, so block can't be made optional"
        next
      end
      unless options[:force]
        # Check if needed
        test_on = options.fetch(:test_on)
        result =  begin
                    test_on.send(selector, *options.fetch(:arg, []))
                  rescue LocalJumpError
                    false
                  end
        next if result.class.name =~ /Enumerator$/
      end
      arity = mod.instance_method(selector).arity
      last_arg = []
      if arity < 0
        last_arg = ["*rest"]
        arity = -1-arity
      end
      arg_sequence = ((0...arity).map{|i| "arg_#{i}"} + last_arg + ["&block"]).join(", ")

      alias_method_chain(mod, selector, :optional_block) do |aliased_target, punctuation|
        mod.module_eval <<-end_eval
          def #{aliased_target}_with_optional_block#{punctuation}(#{arg_sequence})
            return to_enum(:#{aliased_target}_without_optional_block#{punctuation}, #{arg_sequence}) unless block_given?
            #{aliased_target}_without_optional_block#{punctuation}(#{arg_sequence})
          end
        end_eval
      end
    end
  end
  def self.alias_method_chain(mod, target, feature)
  mod.class_eval do
    # Strip out punctuation on predicates or bang methods since
    # e.g. target?_without_feature is not a valid method name.
    aliased_target, punctuation = target.to_s.sub(/([?!=])$/, ''), $1
    yield(aliased_target, punctuation) if block_given?

    with_method, without_method = "#{aliased_target}_with_#{feature}#{punctuation}", "#{aliased_target}_without_#{feature}#{punctuation}"

    alias_method without_method, target
    alias_method target, with_method

    case
      when public_method_defined?(without_method)
        public target
      when protected_method_defined?(without_method)
        protected target
      when private_method_defined?(without_method)
        private target
    end
  end
end
