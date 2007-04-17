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
require 'flare'

class Flare::Context
  def index_info
    Solr::Response::IndexInfo.new(<<RUBY_CODE
    {
     'responseHeader'=>{
      'status'=>0,
      'QTime'=>7},
     'fields'=>{
      'body_zh_text'=>{'type'=>'text_zh'},
      'type_zh_facet'=>{'type'=>'string'},
      'subject_era_facet'=>{'type'=>'string'},
      'call_number_display'=>{'type'=>'text'},
      'id'=>{'type'=>'string'},
      'title_text'=>{'type'=>'text'},
      'isbn_text'=>{'type'=>'text'},
      'source_facet'=>{'type'=>'string'},
      'subject_geographic_facet'=>{'type'=>'string'},
      'author_text'=>{'type'=>'text'},
      'marc_text'=>{'type'=>'text'},
      'body_en_text'=>{'type'=>'text'},
      'author_zh_facet'=>{'type'=>'string'},
      'title_en_text'=>{'type'=>'text'},
      'subject_topic_facet'=>{'type'=>'string'},
      'library_facet'=>{'type'=>'string'},
      'subject_genre_facet'=>{'type'=>'string'},
      'external_url_display'=>{'type'=>'text'},
      'format_facet'=>{'type'=>'string'},
      'type_en_facet'=>{'type'=>'string'},
      'author_en_facet'=>{'type'=>'string'},
      'text'=>{'type'=>'text'},
      'call_number_facet'=>{'type'=>'string'},
      'year_facet'=>{'type'=>'string'},
      'location_facet'=>{'type'=>'string'},
      'title_zh_text'=>{'type'=>'text_zh'}},
     'index'=>{
      'maxDoc'=>1337165,
      'numDocs'=>1337159,
      'version'=>'1174965134952'}}
RUBY_CODE
)
  end
end

class FlareContextTest < Test::Unit::TestCase
  def setup
    @flare_context = Flare::Context.new({:solr_url => 'http://localhost:8983/solr'})
  end
  
  def test_clear
    @flare_context.page = 5
    @flare_context.clear
    assert_equal @flare_context.page, 1
  end
end