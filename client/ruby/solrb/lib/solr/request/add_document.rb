require 'solr/request/base'
require 'solr/document'
require 'rexml/document'

module Solr
  module Request
    class AddDocument < Solr::Request::Base

      # create the request, optionally passing in a Solr::Document
      #
      #   request = Solr::Request::AddDocument.new doc
      #
      # as a short cut you can pass in a Hash instead:
      #
      #   request = Solr::Request.new :creator => 'Jorge Luis Borges'
        
      def initialize(doc={})
        case doc
        when Hash
          @doc = Solr::Document.new(doc)
        when Solr::Document
          @doc = doc
        else
          raise "must pass in Solr::Document or Hash"
        end
      end

      # returns the request as a string suitable for posting
      
      def to_s
        e = REXML::Element.new 'add'
        e.add_element @doc.to_xml
        return e.to_s
      end
    end
  end
end
