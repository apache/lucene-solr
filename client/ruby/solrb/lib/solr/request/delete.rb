require 'rexml/document'

module Solr
  module Request

    class Delete < Solr::Request::Update

      # A delete request can be for a specific document id
      #
      #   request = Solr::Request::Delete.new(:id => 1234)
      #
      # or by query:
      #
      #   request = Solr::Request::Delete.new(:query =>
      #
      def initialize(options)
        unless options.kind_of?(Hash) and (options[:id] or options[:query])
          raise Solr::Exception.new("must pass in :id or :query")
        end
        if options[:id] and options[:query]
          raise Solr::Exception.new("can't pass in both :id and :query")
        end
        @document_id = options[:id]
        @query = options[:query]
      end

      def to_s
        delete_element = REXML::Element.new('delete')
        if @document_id
          id_element = REXML::Element.new('id')
          id_element.text = @document_id
          delete_element.add_element(id_element)
        elsif @query
          query = REXML::Element.new('query')
          query.text = @query 
          delete_element.add_element(query)
        end
        return delete_element.to_s
      end
    end
  end
end

