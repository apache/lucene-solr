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

class Solr::Importer::Mapper
  def initialize(mapping, options={})
    @mapping = mapping
    @options = options
  end
  
  def field_data(orig_data, field_name)
    orig_data[field_name]
  end
  
  def mapped_field_value(orig_data, field_mapping)
    case field_mapping
      when String
        field_mapping
      when Proc
        field_mapping.call(orig_data)
      when Symbol
        field_data(orig_data, @options[:stringify_symbols] ? field_mapping.to_s : field_mapping)
      when Enumerable
        field_mapping.collect {|orig_field_name| mapped_field_value(orig_data, orig_field_name)}.flatten
      else
        raise "Unknown mapping for #{field_mapping}"
    end
  end
  
  def map(orig_data)
    mapped_data = {}
    @mapping.each do |solr_name, field_mapping|
      value = mapped_field_value(orig_data, field_mapping)
      mapped_data[solr_name] = value if value
    end
    
    mapped_data
  end
  
  
  
  
end
