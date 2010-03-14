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
require 'flare'

class Flare::Context
  def index_info
    Solr::Response::IndexInfo.new(
<<SOLR_RESPONSE
    {
     'responseHeader'=>{'status'=>0, 'QTime'=>7},
     'fields'=>{'id'=>{'type'=>'string'}, 'text'=>{'type'=>'text'}},
     'index'=>{'maxDoc'=>1337165, 'numDocs'=>1337159, 'version'=>'1174965134952'}
    }
SOLR_RESPONSE
)
  end
end

class FlareContextTest < Test::Unit::TestCase
  def setup
    @flare_context = Flare::Context.new({:solr_url => 'http://server:8983/solr'})
  end
  
  def test_clear
    @flare_context.page = 5
    @flare_context.clear
    assert_equal @flare_context.page, 1
  end
end