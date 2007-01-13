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

require 'erb'

module Solr
  module Request
    class Select < Solr::Request::Base
      attr_accessor :query

      def initialize(query)
        @query = query
      end
      
      def response_format
        :ruby
      end
      
      def handler
        'select'
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
