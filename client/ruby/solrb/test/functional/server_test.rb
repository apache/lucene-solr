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

class BadRequest < Solr::Request::Standard
  def response_format
    :invalid
  end
end

class ServerTest < Test::Unit::TestCase
  include Solr

  def setup
    @connection = Connection.new("http://localhost:8888/solr")
  end
  
  def test_bad_connection
    conn = Solr::Connection.new 'http://localhost:9999/invalid'
    assert_raise(Errno::ECONNREFUSED) do
      conn.send(Solr::Request::Ping.new)
    end
  end
  
  def test_bad_url
    conn = Solr::Connection.new 'http://localhost:8888/invalid'
    assert_raise(Net::HTTPServerException) do
      conn.send(Solr::Request::Ping.new)
    end
  end
  
  def test_commit
    response = @connection.send(Solr::Request::Commit.new)
    assert_equal "<result status=\"0\"></result>", response.raw_response
  end
  
  def test_ping
    response = @connection.ping
    assert_match /ping/, response.raw_response
  end
  
  def test_invalid_response_format
    request = BadRequest.new(:query => "solr")
    assert_raise(RuntimeError) do
      @connection.send(request)
    end
  end
  
  def test_escaping
    doc = Solr::Document.new :id => 47, :ruby_t => 'puts "ouch!"'
    @connection.send(Solr::Request::AddDocument.new(doc))
    @connection.commit
    
    request = Solr::Request::Standard.new :query => 'ruby_t:ouch'
    result = @connection.send(request)
    
    assert_match /puts/, result.raw_response
  end

end
