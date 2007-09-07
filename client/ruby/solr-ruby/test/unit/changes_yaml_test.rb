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

class ChangesYamlTest < Test::Unit::TestCase
  def test_parse
    change_log = YAML.load_file(File.expand_path(File.dirname(__FILE__)) + "/../../CHANGES.yml")
    assert_equal Date.parse("2007-02-15"), change_log["v0.0.1"]["release_date"]
    assert_equal ["initial release"], change_log["v0.0.1"]["changes"]
  end
end
