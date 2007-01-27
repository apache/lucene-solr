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

# TODO: Maybe replace this with flexmock
class SolrMockBaseTestCase < Test::Unit::TestCase
  include Solr
  
  def setup
    Connection.send(:alias_method, :orig_post, :post)
  end
  
  def teardown
    Connection.send(:alias_method, :post, :orig_post)
  end
    
  def set_post_return(value)
    Connection.class_eval %{
      def post(request)
        %q{#{value}}
      end
    }
  end
  
  def test_dummy
    # So Test::Unit is happy running this class
  end
    
end
