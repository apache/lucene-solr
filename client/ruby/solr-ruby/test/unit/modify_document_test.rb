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

class ModifyDocumentTest < Test::Unit::TestCase

  def test_update_formatting
    request = Solr::Request::ModifyDocument.new(:id => 10, :overwrite => {:name => :value})
    assert_equal :xml, request.response_format
    assert_equal 'update?mode=name:OVERWRITE', request.handler
    
    assert_match(/<add>[\s]*<doc>[\s]*<field name=["']id['"]>10<\/field>[\s]*<field name=['"]name['"]>value<\/field>[\s]*<\/doc>[\s]*<\/add>/, request.to_s)
  end
end
