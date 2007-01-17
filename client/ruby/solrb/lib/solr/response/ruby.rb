module Solr
  module Response

    class Ruby < Solr::Response::Base
      attr_reader :data

      def initialize(ruby_code)
        super(ruby_code)
        begin
          #TODO: what about pulling up data/header/response to ResponseBase,
          #      or maybe a new middle class like SelectResponseBase since
          #      all Select queries return this same sort of stuff??
          #      XML (&wt=xml) and Ruby (&wt=ruby) responses contain exactly the same structure.
          #      a goal of solrb is to make it irrelevant which gets used under the hood, 
          #      but favor Ruby responses.
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
