require 'rexml/xpath'

module Solr
  module Response
    class Ping < Solr::Response::Xml

      def initialize(xml)
        super(xml)
        @ok = REXML::XPath.first(@doc, './solr/ping') ? true : false
      end

      # returns true or false depending on whether the ping
      # was successful or not
      def ok?
        @ok
      end

    end
  end
end
