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

class Solr::Request::Base
  
  
  #TODO : Add base support for the debugQuery flag, and such that the response provides debug output easily

  # returns either :xml or :ruby depending on what the
  # response type is for a given request
  
  def response_format
    raise "unknown request type: #{self.class}"
  end
  
  def content_type
    'text/xml; charset=utf-8'
  end

  # returns the solr handler or url fragment that can 
  # respond to this type of request
  
  def handler
    raise "unknown request type: #{self.class}"
  end

end
