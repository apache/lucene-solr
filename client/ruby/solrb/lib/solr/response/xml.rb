require 'rexml/document'
require 'solr/exception'

module Solr
  module Response

    class Xml < Solr::Response::Base
      attr_reader :doc, :status_code, :status_message

      def initialize(xml)
        super(xml)
        begin
          # parse the xml
          @doc = REXML::Document.new(xml)
          # look for the result code and string 
          result = REXML::XPath.first(@doc, './result')
          if result
            @status_code =  result.attributes['status']
            @status_message = result.text
          end
        rescue REXML::ParseException => e
          raise Solr::Exception.new("invalid response xml: #{e}")
        end
      end

      def ok?
        return @status_code == '0'
      end

    end

  end
end
