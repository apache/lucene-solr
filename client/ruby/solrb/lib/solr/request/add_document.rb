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

require 'solr/request/base'
require 'solr/document'
require 'rexml/document'

module Solr
  module Request
    class AddDocument < Solr::Request::Base

      # create the request, optionally passing in a Solr::Document
      #
      #   request = Solr::Request::AddDocument.new doc
      #
      # as a short cut you can pass in a Hash instead:
      #
      #   request = Solr::Request.new :creator => 'Jorge Luis Borges'
        
      def initialize(doc={})
        case doc
        when Hash
          @doc = Solr::Document.new(doc)
        when Solr::Document
          @doc = doc
        else
          raise "must pass in Solr::Document or Hash"
        end
      end

      # returns the request as a string suitable for posting
      
      def to_s
        e = REXML::Element.new 'add'
        e.add_element @doc.to_xml
        return e.to_s
      end
    end
  end
end
