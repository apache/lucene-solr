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

class Solr::Response::Standard < Solr::Response::Ruby
  FacetValue = Struct.new(:name, :value)
  include Enumerable
  
  def initialize(ruby_code)
    super
    @response = @data['response']
    raise "response section missing" unless @response.kind_of? Hash
  end

  def total_hits
    @response['numFound']
  end

  def start
    @response['start']
  end

  def hits
    @response['docs']
  end

  def max_score
    @response['maxScore']
  end
  
  # TODO: consider the use of json.nl parameter
  def field_facets(field)
    facets = []
    values = @data['facet_counts']['facet_fields'][field]
    Solr::Util.paired_array_each(values) do |key, value|
      facets << FacetValue.new(key, value)
    end
    
    facets
  end
  
  def highlighted(id, field)
    @data['highlighting'][id.to_s][field.to_s] rescue nil
  end
  
  # supports enumeration of hits
  # TODO revisit - should this iterate through *all* hits by re-requesting more?
  def each
    @response['docs'].each {|hit| yield hit}
  end

end
