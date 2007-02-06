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

require 'solr_mock_base'

class DeleteTest <  SolrMockBaseTestCase

  def test_delete_request
    request = Solr::Request::Delete.new(:id => '123')
    assert_match(/<delete>[\s]*<id>123<\/id>[\s]*<\/delete>/m, request.to_s)
  end

  def test_delete_by_query_request
    request = Solr::Request::Delete.new(:query => 'name:summers')
    assert_match(/<delete>[\s]*<query>name:summers<\/query>[\s]*<\/delete>/m, request.to_s)
  end

  def test_delete_response
    conn = Solr::Connection.new 'http://localhost:9999/solr'
    set_post_return('<result status="0"></result>')
    response = conn.send(Solr::Request::Delete.new(:id => 123))
    assert_equal true, response.ok? 
  end
  
  def test_bad_delete_request
    assert_raise(Solr::Exception) do
      Solr::Request::Delete.new(:bogus => :param)
    end

    assert_raise(Solr::Exception) do
      Solr::Request::Delete.new(:id => 529, :query => "id:529")
    end
  end

  def test_bad_delete_response
    conn = Solr::Connection.new 'http://localhost:9999/solr'
    set_post_return('<result status="400">uhoh</result>')
    response = conn.send(Solr::Request::Delete.new(:id => 123))
    assert_equal false, response.ok? 
  end

  def test_delete_by_i18n_query_request
    request = Solr::Request::Delete.new(:query => 'ëäïöü')
    assert_match(/<delete>[\s]*<query>ëäïöü<\/query>[\s]*<\/delete>/m, request.to_s)
  end

end
