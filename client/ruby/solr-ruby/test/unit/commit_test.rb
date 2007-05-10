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

class CommitTest < SolrMockBaseTestCase

  def test_commit
    xml = '<?xml version="1.0" encoding="UTF-8"?><response><lst name="responseHeader"><int name="status">0</int><int name="QTime">2</int></lst></response>'
    conn = Solr::Connection.new('http://localhost:9999/solr')
    set_post_return(xml)
    response = conn.send(Solr::Request::Commit.new)
    assert_kind_of Solr::Response::Commit, response
    assert_equal true, response.ok?

    # test shorthand
    assert_equal true, conn.commit
  end

  # def test_invalid_commit
  #   xml = '<?xml version="1.0" encoding="UTF-8"?><response><lst name="responseHeader"><int name="status">1</int><int name="QTime">2</int></lst></response>'
  #   conn = Solr::Connection.new('http://localhost:9999/solr')
  #   set_post_return(xml)
  #   response = conn.send(Solr::Request::Commit.new)
  #   assert_kind_of Solr::Response::Commit, response
  #   assert_equal false, response.ok?
  # 
  #   # test shorthand
  #   assert_equal false, conn.commit
  #  end

end
