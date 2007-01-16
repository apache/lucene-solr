module Solr
  module Response

    class AddDocument < Solr::Response::Xml
      def initialize(xml)
        super(xml)
      end
    end

  end
end
