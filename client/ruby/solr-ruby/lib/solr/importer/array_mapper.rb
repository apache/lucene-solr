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



class Solr::Importer::ArrayMapper < Solr::Importer::Mapper
  # TODO document that initializer takes an array of Mappers [mapper1, mapper2, ... mapperN]
  
  # TODO: make merge conflict handling configurable.  as is, the last map fields win.
  def map(orig_data_array)
    mapped_data = {}
    orig_data_array.each_with_index do |data,i|
      mapped_data.merge!(@mapping[i].map(data))
    end
    mapped_data
  end
end