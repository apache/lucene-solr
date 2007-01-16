module Solr
  module Response
    class Standard < Solr::Response::Ruby
      include Enumerable

      def initialize(ruby_code)
        super(ruby_code)
      end

      def total_hits
        return @response['numFound']
      end

      def start
        return @response['start']
      end

      def hits
        return @response['docs']
      end

      def max_score
        return @response['maxScore']
      end

      # supports enumeration of hits
      def each
        @response['docs'].each {|hit| yield hit}
      end

    end
  end
end
