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

begin
  require 'solr'
  require 'test/unit'
  require 'hpricot'

  class HpricotMapperTest < Test::Unit::TestCase
  
    def setup
      @doc = open(File.expand_path(File.dirname(__FILE__)) + "/hpricot_test_file.xml"){|f| Hpricot.XML(f)}
    end

    def test_simple_hpricot_path
      mapping = {:field1 => :'child[@attribute="attribute1"]',
                 :field2 => :'child[@attribute="attribute2"]',
                 :field3 => :'child[@attribute="attribute3"]',
                 :field4 => :'child[@attribute="attribute3"] grandchild',
                 :field5 => :'child'}    
    
      mapper = Solr::Importer::HpricotMapper.new(mapping)    
      mapped_data = mapper.map(@doc)
        
      assert_equal ['text1'], mapped_data[:field1]
      assert_equal ['text2'], mapped_data[:field2]
      assert_equal ['text3<grandchild>grandchild 3 text</grandchild>'], mapped_data[:field3]
      assert_equal ['grandchild 3 text'], mapped_data[:field4]
      assert_equal ['text1', 'text2', 'text3<grandchild>grandchild 3 text</grandchild>'], mapped_data[:field5]
    end

  end
rescue LoadError => e
  puts "HpricotMapperTest not run because #{e}"
end