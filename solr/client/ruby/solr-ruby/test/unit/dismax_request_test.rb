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

class DismaxRequestTest < Test::Unit::TestCase
  
  def test_basic_query
    request = Solr::Request::Dismax.new(:query => 'query', :phrase_slop => '1000', :sort => [{:deedle => :descending}])
    assert_match(/q=query/, request.to_s)
    assert_match(/qt=dismax/, request.to_s)
    assert_match(/ps=1000/, request.to_s)
    assert_match(/sort=deedle%20desc/, request.to_s)
  end
  
end