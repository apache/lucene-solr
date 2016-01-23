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
    assert_match(/<commit waitSearcher=["']true["'] waitFlush=["'']true["'']\/>/, request.to_s)
  end
  
  def test_add_doc_request
    request = Solr::Request::AddDocument.new(:title => "title")
    assert_match(/<add>[\s]*<doc>[\s]*<field name=["']title["']>title<\/field>[\s]*<\/doc>[\s]*<\/add>/m, request.to_s)
    assert_equal :xml, request.response_format
    assert_equal 'update', request.handler
    
    assert_raise(RuntimeError) do
      Solr::Request::AddDocument.new("invalid")
    end
  end
  
  def test_add_multidoc_request
    request = Solr::Request::AddDocument.new([{:title => "title1"}, {:title => "title2"}])
    assert_match(/<add>[\s]*<doc>[\s]*<field name=["']title["']>title1<\/field>[\s]*<\/doc>[\s]*<doc>[\s]*<field name=["']title["']>title2<\/field>[\s]*<\/doc>[\s]*<\/add>/m, request.to_s)
    assert_equal :xml, request.response_format
    assert_equal 'update', request.handler
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
