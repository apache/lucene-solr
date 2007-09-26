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

class SpellcheckRequestTest < Test::Unit::TestCase
  def test_spellcheck_request
    request = Solr::Request::Spellcheck.new(:query => 'whateva', :suggestion_count => 5, :accuracy => 0.7, :only_more_popular => true)
    assert_equal :ruby, request.response_format
    assert_equal 'select', request.handler
    hash = request.to_hash
    assert_equal 'whateva', hash[:q]
    assert_equal 5, hash[:suggestionCount]
    assert_equal 0.7, hash[:accuracy]
    assert_equal true, hash[:onlyMorePopular]
  end
end
