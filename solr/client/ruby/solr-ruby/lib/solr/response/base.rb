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

class Solr::Response::Base
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
    
    begin
      klass = eval(request.class.name.sub(/Request/,'Response'))
    rescue NameError
      raise Solr::Exception.new("unknown request type: #{request.class}")
    else
      klass.new(raw)
    end
    
  end

end
