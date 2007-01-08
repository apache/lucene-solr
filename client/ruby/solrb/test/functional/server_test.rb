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

require 'test/unit'
require 'solr'

class TestServer < Test::Unit::TestCase
  include Solr

  def setup
    @connection = Connection.new("http://localhost:8888")
  end
  
  def test_commit
    response = @connection.send(UpdateRequest.new("<commit/>"))
    assert_equal "<result status=\"0\"></result>", response.raw_response
  end
  
  def test_escaping
    doc = {:id => 47, :ruby_t => 'puts "ouch!"'}
    request = AddDocumentRequest.new(doc)
    @connection.send(request)
    
    @connection.send(UpdateRequest.new("<commit/>"))
    
    request = StandardRequest.new
    request.query = "ruby_t:ouch"
    request.field_list="*,score"
    result = @connection.send(request)
    
    assert result.raw_response =~ /puts/
  end
end
