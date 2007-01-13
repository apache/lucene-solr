require 'rexml/document'

module Solr
  module Request
    class Commit < Solr::Request::Base

      def to_s
        return REXML::Element.new('commit').to_s
      end

    end
  end
end
