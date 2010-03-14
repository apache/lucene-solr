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

# TODO: Consider something lazy like this?
# Solr::Request::Ping = Solr::Request.simple_request :format=>:xml, :handler=>'admin/ping'
# class Solr::Request
#   def self.simple_request(options)
#     Class.new do 
#       def response_format
#         options[:format]
#       end
#       def handler
#         options[:handler]
#       end
#     end
#   end
# end

class Solr::Request::Ping < Solr::Request::Base
  def response_format
    :xml
  end
  
  def handler
    'admin/ping'
  end
end
