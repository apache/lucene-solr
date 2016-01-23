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

class DataMapperTest < Test::Unit::TestCase

  def test_static_mapping
    mapping = {:static => "value",
               :static_array => ["value1", "value2"]}
    
    mapper = Solr::Importer::Mapper.new(mapping)    
    mapped_data = mapper.map({})
        
    assert_equal "value", mapped_data[:static]
    assert_equal ["value1", "value2"], mapped_data[:static_array]
  end

  def test_simple_mapping
    orig_data = {:orig_field => "value",
                 :multi1 => "val1", :multi2 => "val2"}
    mapping = {:solr_field => :orig_field,
               :mapped_array => [:multi1, :multi2], }    
    
    mapper = Solr::Importer::Mapper.new(mapping)    
    mapped_data = mapper.map(orig_data)
        
    assert_equal "value", mapped_data[:solr_field]
    assert_equal ["val1", "val2"], mapped_data[:mapped_array]
  end
  
  def test_proc
    orig_data = {:orig_field => "value"}
    mapping = {:solr_field => Proc.new {|record| ">#{record[:orig_field]}<"}}
    
    mapper = Solr::Importer::Mapper.new(mapping)    
    mapped_data = mapper.map(orig_data)
        
    assert_equal  ">value<", mapped_data[:solr_field]
  end
    
  def test_overridden_field
    mapping = {:solr_field => [:orig_field1, :orig_field2]}
    orig_data = {:orig_field1 => "value1", :orig_field2 => "value2", }
    
    mapper = Solr::Importer::Mapper.new(mapping)
    def mapper.field_data(orig_data, field_name)
      ["~#{super(orig_data, field_name)}~"]  # array tests that the value is flattened
    end    
    mapped_data = mapper.map(orig_data)
        
    assert_equal ["~value1~", "~value2~"], mapped_data[:solr_field]
  end
  
  def test_unknown_mapping
    mapping = {:solr_field => /foo/}  # regexp currently not a valid mapping type
    
    mapper = Solr::Importer::Mapper.new(mapping)
    
    assert_raise(RuntimeError) do
      mapped_data = mapper.map({})
    end
  end

end
