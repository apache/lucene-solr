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

include Solr::Importer

class ArrayMapperTest < Test::Unit::TestCase
  def test_simple
    mapping1 = {:one => "uno"}
    mapping2 = {:two => "dos"}
    
    mapper = Solr::Importer::ArrayMapper.new([Mapper.new(mapping1),Mapper.new(mapping2)])    
    mapped_data = mapper.map([{},{}])
    assert_equal "uno", mapped_data[:one]
    assert_equal "dos", mapped_data[:two]
  end
  
  def test_field_conflict_goes_to_last
    mapping1 = {:same => "uno"}
    mapping2 = {:same => "dos"}
    
    mapper = Solr::Importer::ArrayMapper.new([Mapper.new(mapping1),Mapper.new(mapping2)])    
    mapped_data = mapper.map([{},{}])
    assert_equal "dos", mapped_data[:same]
  end
end