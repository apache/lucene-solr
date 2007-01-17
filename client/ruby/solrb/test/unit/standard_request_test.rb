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

require 'test/unit'
require 'solr'

class StandardRequestTest < Test::Unit::TestCase

  def test_basic_query
    request = Solr::Request::Standard.new(:query => 'query')
    assert_equal :ruby, request.response_format
    assert_equal 'select', request.handler
    assert_equal 'query', request.to_hash[:q]
    assert_match /q=query/, request.to_s
  end
  
  def test_bad_params
    assert_raise(RuntimeError) do
      Solr::Request::Standard.new(:foo => "invalid")
    end
    
    assert_raise(RuntimeError) do
      Solr::Request::Standard.new(:query => "valid", :foo => "invalid")
    end
  end
  
  def test_common_params
    request = Solr::Request::Standard.new(:query => 'query', :start => 10, :rows => 50,
           :filter_queries => ['fq1', 'fq2'], :field_list => ['id','title','score'])
    assert_equal 10, request.to_hash[:start]
    assert_equal 50, request.to_hash[:rows]
    assert_equal ['fq1','fq2'], request.to_hash[:fq]
    assert_equal "id,title,score", request.to_hash[:fl]
  end
    
  def test_missing_params
    request = Solr::Request::Standard.new(:query => 'query', :debug_query => false, :facets => {:fields =>[:category_facet]})
    assert_nil request.to_hash[:rows]
    assert_no_match /rows/, request.to_s
    assert_no_match /facet\.sort/, request.to_s
    assert_match /debugQuery/, request.to_s
  end
  
  def test_facet_params_all
    request = Solr::Request::Standard.new(:query => 'query',
       :facets => {
         :fields => [:genre,
                     {:year => {:limit => 50, :mincount => 0, :missing => false, :sort => :term}}], # field that overrides the global facet parameters
         :queries => ["q1", "q2"],
         :limit => 5, :zeros => true, :mincount => 20, :sort => :count  # global facet parameters
        }
    )
    assert_equal true, request.to_hash[:facet]
    assert_equal [:genre, :year], request.to_hash[:"facet.field"]
    assert_equal ["q1", "q2"], request.to_hash[:"facet.query"]
    assert_equal 5, request.to_hash[:"facet.limit"]
    assert_equal 20, request.to_hash[:"facet.mincount"]
    assert_equal true, request.to_hash[:"facet.sort"]
    assert_equal 50, request.to_hash[:"f.year.facet.limit"]
    assert_equal 0, request.to_hash[:"f.year.facet.mincount"]
    assert_equal false, request.to_hash[:"f.year.facet.sort"]
  end

  def test_basic_sort
    request = Solr::Request::Standard.new(:query => 'query', :sort => [{:title => :descending}])
    assert_equal 'query;title desc', request.to_hash[:q]
  end

end
