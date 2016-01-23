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

class PingTest < SolrMockBaseTestCase 

  def test_ping_response
    xml = 
<<PING_RESPONSE

<?xml-stylesheet type="text/xsl" href="ping.xsl"?>

<solr>
  <ping>

  </ping>
</solr>
PING_RESPONSE
    conn = Solr::Connection.new('http://localhost:9999')
    set_post_return(xml)
    response = conn.send(Solr::Request::Ping.new)
    assert_kind_of Solr::Response::Ping, response
    assert_equal true, response.ok? 

    # test shorthand
    assert true, conn.ping
  end

  def test_bad_ping_response
    xml = "<foo>bar</foo>"
    conn = Solr::Connection.new('http://localhost:9999')
    set_post_return(xml)
    response = conn.send(Solr::Request::Ping.new)
    assert_kind_of Solr::Response::Ping, response
    assert_equal false, response.ok?

    # test shorthand
    assert_equal false, conn.ping
  end

end
