# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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

        # TODO: Factor out this case... perhaps the request object should provide the response class instead?  Or dynamically align by class name?
        #       Maybe the request itself could have the response handling features that get mixed in with a single general purpose response object?
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
        when Solr::Request::IndexInfo
          return Solr::Response::IndexInfo.new(raw)
        else
          raise Solr::Exception.new("unknown request type: #{request.class}")
        end
      end

    end
  end
end
