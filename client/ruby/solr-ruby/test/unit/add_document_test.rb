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

class AddDocumentTest < SolrMockBaseTestCase

  def test_add_document_response
    conn = Solr::Connection.new('http://localhost:9999/solr')
    set_post_return('<result status="0"></result>')
    doc = {:id => '123', :text => 'Tlon, Uqbar, Orbis Tertius'}
    response = conn.send(Solr::Request::AddDocument.new(doc))
    assert_equal true, response.ok?
  end

  def test_bad_add_document_response
    conn = Solr::Connection.new('http://localhost:9999/solr')
    set_post_return('<result status="400"></result>')
    doc = {:id => '123', :text => 'Tlon, Uqbar, Orbis Tertius'}
    response = conn.send(Solr::Request::AddDocument.new(doc))
    assert_equal false, response.ok?
  end

  def test_shorthand
    conn = Solr::Connection.new('http://localhost:9999/solr')
    set_post_return('<result status="0"></result>')
    doc = {:id => '123', :text => 'Tlon, Uqbar, Orbis Tertius'}
    assert_equal true, conn.add(:id => '123', :text => 'Tlon, Uqbar, Orbis Tetius')
  end

end
