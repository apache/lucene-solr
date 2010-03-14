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

require 'solr'
require 'test/unit'

class DelimitedFileSourceTest < Test::Unit::TestCase

  def test_load
    filename = File.expand_path(File.dirname(__FILE__)) + "/tab_delimited.txt"
    
    source = Solr::Importer::DelimitedFileSource.new(filename,/\t/)
    assert_equal source.to_a.size, 1
    
    source.each do |data|
       assert_equal data[:asin], '0865681740'
    end
  end

end
