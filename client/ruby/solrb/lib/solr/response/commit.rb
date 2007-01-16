require 'rexml/xpath'

module Solr
  module Response
    class Commit < Solr::Response::Xml
      attr_reader :ok

      def initialize(xml)
        super(xml)
        e = REXML::XPath.first(@doc, './result')
        if e and e.attributes['status'] == '0'
          @ok = true
        else
          @ok = false
        end
      end

      def ok?
        @ok
      end
    end
  end
end

