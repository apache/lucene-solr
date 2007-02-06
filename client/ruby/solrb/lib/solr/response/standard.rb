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
    super(ruby_code)
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
  
  def field_facets(field)
    facets = []
    values = @data['facet_counts']['facet_fields'][field]
    0.upto(values.size / 2 - 1) do |i|
      n = i * 2
      facets << FacetValue.new(values[n], values[n+1])
    end
    
    facets
  end
  

  # supports enumeration of hits
  # TODO revisit - should this iterate through *all* hits by re-requesting more?
  def each
    @response['docs'].each {|hit| yield hit}
  end

end
