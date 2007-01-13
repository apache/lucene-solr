require 'rexml/document'

module Solr
  class Field
    attr_accessor :name
    attr_accessor :value

    def initialize(key_val, opts={})
      raise "first argument must be a hash" unless key_val.kind_of? Hash
      @name = key_val.keys[0].to_s
      @value = key_val.values[0].to_s
    end

    def to_xml
      e = REXML::Element.new 'field'
      e.attributes['name'] = @name
      e.text = @value
      return e
    end

  end
end
