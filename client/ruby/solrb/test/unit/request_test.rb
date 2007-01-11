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

class RequestTest < Test::Unit::TestCase
  def test_basic_params
    request = Solr::StandardRequest.new
    assert_equal("/solr/select", request.url_path)

    request.query = "term"
    assert_equal "term", request.to_hash[:q]
    
    body = request.to_http_body
    assert body =~ /q=term/ 
  end
  
  def test_update_request
    request = Solr::UpdateRequest.new("<commit/>")
    assert_equal("/solr/update", request.url_path)
  end
  
  def test_add_doc_request
    request = Solr::AddDocumentRequest.new({:title => "title"})
    assert_equal("<add><doc><field name='title'>title</field></doc></add>", request.to_http_body)
  end
  
  def test_response_error
    assert_raise(Solr::RequestException) do
      new Solr::Response.new("<result status=\"400\">ERROR:</result>")
    end
  end
  
end
