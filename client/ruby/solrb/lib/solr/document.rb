require 'rexml/document'
require 'solr/field'

module Solr
  class Document
    include Enumerable

    # Create a new Solr::Document, optionally passing in a hash of 
    # key/value pairs for the fields
    #
    #   doc = Solr::Document.new(:creator => 'Jorge Luis Borges')
    def initialize(hash={})
      @fields = []
      hash.each_pair {|k,v| @fields << Solr::Field.new(k => v)}
    end

    # Append a Solr::Field
    #
    #   doc << Solr::Field.new(:creator => 'Jorge Luis Borges')
    #
    # If you are truly lazy you can simply pass in a hash:
    #
    #   doc << {:creator => 'Jorge Luis Borges'}
    def <<(field)
      case field
      when Hash
        @fields << Solr::Field.new(field)
      when Solr::Field
        @fields << field
      else
        raise "must pass in Solr::Field or Hash"
      end
    end

    # shorthand to allow hash lookups
    #   doc['name']
    def [](name)
      field = @fields.find {|f| f.name == name.to_s}
      return field.value if field
      return nil
    end

    # shorthand to assign as a hash
    def []=(name,value)
      @fields << Solr::Field.new(name => value)
    end

    # convert the Document to a REXML::Element 
    def to_xml
      e = REXML::Element.new 'doc'
      @fields.each {|f| e.add_element(f.to_xml)}
      return e
    end
  end
end
