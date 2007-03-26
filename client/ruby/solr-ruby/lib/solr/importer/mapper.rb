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

# For files with the first line containing field names
class Solr::Importer::Mapper
  def initialize(mapping)
    @mapping = mapping
  end
  
  def field_data(orig_data, field_name)
    case field_name
      when Symbol
        orig_data[field_name]
      else
        field_name
    end
  end
  
  def map(orig_data)
    mapped_data = {}
    @mapping.each do |solr_name, field_mapping|
      value = case field_mapping
        when Proc
          field_mapping.call(orig_data)
        when String, Symbol
          field_data(orig_data, field_mapping)
        when Enumerable
          field_mapping.collect {|orig_field_name| field_data(orig_data, orig_field_name)}.flatten
        else
          raise "Unknown mapping for #{solr_name}: #{field_mapping}"
      end
      mapped_data[solr_name] = value if value
    end
    
    mapped_data
  end
  
  
end
