require 'erb'

module Solr
  module Request
    class Select < Solr::Request::Base
      attr_accessor :query

      def initialize(query)
        @query = query
      end

      def to_hash
        return {:q => query, :wt => 'ruby', :fl => '*,score'}
      end

      def to_s
        raw_params = self.to_hash

        http_params = []
        raw_params.each do |key,value|
        http_params << "#{key}=#{ERB::Util::url_encode(value)}" if value
        end

        http_params.join("&")
      end

    end
  end
end
