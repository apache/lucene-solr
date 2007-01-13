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

class BadRequest < Solr::Request::Base
end

class RequestTest < Test::Unit::TestCase

  def test_commit_request
    request = Solr::Request::Commit.new
    assert_equal :xml, request.response_format
    assert_equal 'update', request.handler
    assert_equal '<commit/>', request.to_s
  end
  
  def test_add_doc_request
    request = Solr::Request::AddDocument.new(:title => "title")
    assert_equal "<add><doc><field name='title'>title</field></doc></add>", request.to_s
    assert :xml, request.response_format
    assert 'update', request.handler
    
    assert_raise(RuntimeError) do
      Solr::Request::AddDocument.new("invalid")
    end
  end

  def test_select_request
    request = Solr::Request::Select.new('belkin')
    assert_equal :ruby, request.response_format
    assert 'select', request.handler
    assert 'belkin', request.to_hash['q']
    assert_match /q=belkin/, request.to_s
  end
  
  def test_ping_request
    request = Solr::Request::Ping.new
    assert_equal :xml, request.response_format
  end

  def test_bad_request_class
    assert_raise(RuntimeError) do
      BadRequest.new.response_format
    end
    
    assert_raise(RuntimeError) do
      BadRequest.new.handler
    end
  end

end
