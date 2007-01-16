module Solr
  module Response
    class Base
      attr_reader :raw_response

      def initialize(raw_response)
        @raw_response = raw_response
      end

      # factory method for creating a Solr::Response::* from 
      # a request and the raw response content
      def self.make_response(request, raw)

        # make sure response format seems sane
        unless [:xml, :ruby].include?(request.response_format)
          raise Solr::Exception.new("unknown response format: #{request.response_format}" )
        end

        case request
        when Solr::Request::Ping
          return Solr::Response::Ping.new(raw)
        when Solr::Request::AddDocument
          return Solr::Response::AddDocument.new(raw)
        when Solr::Request::Commit
          return Solr::Response::Commit.new(raw)
        when Solr::Request::Standard
          return Solr::Response::Standard.new(raw)
        when Solr::Request::Delete
          return Solr::Response::Delete.new(raw)
        else
          raise Solr::Exception.new("unknown request type: #{request.class}")
        end
      end

    end
  end
end
