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
    
    assert_raise(RuntimeError) do
      Solr::Request::Standard.new(:query => "valid", :operator => :bogus)
    end
  end
  
  def test_common_params
    request = Solr::Request::Standard.new(:query => 'query', :start => 10, :rows => 50,
           :filter_queries => ['fq1', 'fq2'], :field_list => ['id','title','score'], :operator => :and)
    assert_equal 10, request.to_hash[:start]
    assert_equal 50, request.to_hash[:rows]
    assert_equal ['fq1','fq2'], request.to_hash[:fq]
    assert_equal "id,title,score", request.to_hash[:fl]
    assert_equal "AND", request.to_hash["q.op"]
  end
    
  def test_missing_params
    request = Solr::Request::Standard.new(:query => 'query', :debug_query => false, :facets => {:fields =>[:category_facet]})
    assert_nil request.to_hash[:rows]
    assert_no_match /rows/, request.to_s
    assert_no_match /facet\.sort/, request.to_s
    assert_match /debugQuery/, request.to_s
  end
  
  def test_only_facet_query
    request = Solr::Request::Standard.new(:query => 'query',
       :facets => {
         :queries => ["q1", "q2"],
        }
    )
    
    hash = request.to_hash
    assert_equal ["q1", "q2"], hash["facet.query"]
  end
  
  def test_facet_params_all
    request = Solr::Request::Standard.new(:query => 'query',
       :facets => {
         :fields => [:genre,           
                     # field that overrides the global facet parameters
                     {:year => {:limit => 50, :mincount => 0, :missing => false, :sort => :term, :prefix=>"199", :offset => 7}}], 
         :queries => ["q1", "q2"],
         :prefix => "cat",
         :offset => 3, :limit => 5, :zeros => true, :mincount => 20, :sort => :count  # global facet parameters
        }
    )
    
    hash = request.to_hash
    assert_equal true, hash[:facet]
    assert_equal [:genre, :year], hash["facet.field"]
    assert_equal ["q1", "q2"], hash["facet.query"]
    assert_equal 5, hash["facet.limit"]
    assert_equal 20, hash["facet.mincount"]
    assert_equal true, hash["facet.sort"]
    assert_equal "cat", hash["facet.prefix"]
    assert_equal 50, hash["f.year.facet.limit"]
    assert_equal 0, hash["f.year.facet.mincount"]
    assert_equal false, hash["f.year.facet.sort"]
    assert_equal "199", hash["f.year.facet.prefix"]
    assert_equal 3, hash["facet.offset"]
    assert_equal 7, hash["f.year.facet.offset"]
  end

  def test_basic_sort
    request = Solr::Request::Standard.new(:query => 'query', :sort => [{:title => :descending}])
    assert_equal 'query;title desc', request.to_hash[:q]
  end
  
  def test_highlighting
    request = Solr::Request::Standard.new(:query => 'query',
      :highlighting => {
        :field_list => ['title', 'author'],
        :max_snippets => 3,
        :require_field_match => true,
        :prefix => "<blink>",
        :suffix => "</blink>",
        :fragment_size => 300
      }
    )
    
    hash = request.to_hash
    assert_equal true, hash[:hl]
    assert_equal "title,author", hash["hl.fl"]
    assert_equal true, hash["hl.requireFieldMatch"]
    assert_equal "<blink>", hash["hl.simple.pre"]
    assert_equal "</blink>", hash["hl.simple.post"]
    assert_equal 300, hash["hl.fragsize"]
  end
  
  def test_mlt
    request = Solr::Request::Standard.new(:query => 'query',
      :mlt => {
        :count => 5, :field_list => ['field1', 'field2'],
        :min_term_freq => 3, :min_doc_freq => 10,
        :min_word_length => 4, :max_word_length => 17,
        :max_query_terms => 20, :max_tokens_parsed => 100,
        :boost => true
      }
    )
    
    hash = request.to_hash
    assert_equal true, hash[:mlt]
    assert_equal 5, hash["mlt.count"]
    assert_equal 'field1,field2', hash["mlt.fl"]
    assert_equal 3, hash["mlt.mintf"]
    assert_equal 10, hash["mlt.mindf"]
    assert_equal 4, hash["mlt.minwl"]
    assert_equal 17, hash["mlt.maxwl"]
    assert_equal 20, hash["mlt.maxqt"]
    assert_equal 100, hash["mlt.maxntp"]
    assert_equal true, hash["mlt.boost"]
  end

end
