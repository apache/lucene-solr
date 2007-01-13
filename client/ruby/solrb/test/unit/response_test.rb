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

class ResponseTest < Test::Unit::TestCase

  def test_response_error
    assert_raise(Solr::RequestException) do
      new Solr::Response.new("<result status=\"400\">ERROR:</result>")
    end
    
    begin
      new Solr::Response.new("<result status=\"400\">ERROR:</result>")
    rescue Solr::RequestException => exception
      assert_equal "ERROR:", exception.message
      assert_equal exception.message, exception.to_s
      assert_equal "400", exception.code
    end
  end

end
