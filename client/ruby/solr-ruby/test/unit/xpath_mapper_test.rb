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
  require 'xml/libxml'

  class XPathMapperTest < Test::Unit::TestCase
  
    def setup
      @doc = XML::Document.file(File.expand_path(File.dirname(__FILE__)) + "/xpath_test_file.xml")
    end

    def test_simple_xpath
      mapping = {:solr_field1 => :'/root/parent/child',
                 :solr_field2 => :'/root/parent/child/@attribute'}    
    
      mapper = Solr::Importer::XPathMapper.new(mapping)    
      mapped_data = mapper.map(@doc)
        
      assert_equal ['text1', 'text2'], mapped_data[:solr_field1]
      assert_equal ['attribute1', 'attribute2'], mapped_data[:solr_field2]
    end

  end
rescue LoadError => e
  puts "XPathMapperTest not run because #{e}"
end