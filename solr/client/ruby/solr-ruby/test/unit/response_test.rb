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
require 'solr_mock_base'


class ResponseTest < SolrMockBaseTestCase

  def test_response_xml_error
    begin
      Solr::Response::Xml.new("<broken>invalid xml&")
      flunk("failed to get Solr::Exception as expected") 
    rescue Exception => exception
      assert_kind_of Solr::Exception, exception
      assert_match 'invalid response xml', exception.to_s
    end
  end
  
  def test_invalid_ruby
    assert_raise(Solr::Exception) do
      Solr::Response::Ruby.new(' {...')
    end
  end

  # This is now an acceptable use of Select, for the default request handler with no parameters (other than &wt=ruby)  
  # def test_bogus_request_handling
  #   assert_raise(Solr::Exception) do
  #     Solr::Response::Base.make_response(Solr::Request::Select.new, "response data")
  #   end
  # end

end
