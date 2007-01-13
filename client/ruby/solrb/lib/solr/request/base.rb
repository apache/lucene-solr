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
  module Request
    class Base

      # returns either :xml or :ruby depending on what the
      # response type is for a given request
      
      def response_format
        case self
        when Solr::Request::Commit
          return :xml
        when Solr::Request::Update
          return :xml
        when Solr::Request::Select
          return :ruby
        when Solr::Request::AddDocument
          return :xml
        when Solr::Request::Ping
          return :xml
        else
          raise "unkown request type: #{self.class}"
        end
      end

      # returns the solr handler or url fragment that can 
      # respond to this type of request
      
      def handler
        case self
        when Solr::Request::Commit
          return 'update'
        when Solr::Request::Update
          return 'update'
        when Solr::Request::Select
          return 'select' 
        when Solr::Request::AddDocument
          return 'update'
        when Solr::Request::Ping
          return 'admin/ping'
        else
          raise "unkown request type: #{self.class}"
        end
      end

    end
  end
end
