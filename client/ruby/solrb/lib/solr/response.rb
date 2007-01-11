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
  class Response
    attr_reader :header, :raw_response, :data
    def initialize(body)
      @raw_response = body
      if match = /^<result status="(\d+)"/.match(body)
        unless 0 == match.captures.first.to_i
          error = REXML::Document.new(body).root
          raise RequestException.new(error.attributes["status"], error.text)
        end
      end
    end
  end
  
  class RubyResponse < Response
    def initialize(body)
      super(body)
      parsed_response = eval(body)
      @header = parsed_response['responseHeader']
      @data = parsed_response['response']
    end
  end
  
  class XmlResponse < Response
    def initialize(body)
      super(body)
    end
  end
end
