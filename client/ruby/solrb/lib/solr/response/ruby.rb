module Solr
  module Response

    class Ruby < Solr::Response::Base
      attr_reader :data

      def initialize(ruby_code)
        super(ruby_code)
        begin
          @data = eval(ruby_code)
          @header = @data['responseHeader']
          @response = @data['response']
          raise "response should be a hash" unless @data.kind_of? Hash
          raise "response header missing" unless @header.kind_of? Hash
          raise "response section missing" unless @response.kind_of? Hash
        rescue Exception => e
          raise Solr::Exception.new("invalid ruby code: #{e}")
        end
      end

      def ok?
        return @header['status'] == 0
      end

      def query_time
        return @header['QTime']
      end

    end

  end
end
