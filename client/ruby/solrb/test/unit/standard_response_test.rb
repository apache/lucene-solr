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

require 'solr_mock_base'

class StandardResponseTest <  SolrMockBaseTestCase

  def test_basic
    ruby_code = 
<<RUBY_CODE
{
     'responseHeader'=>{
      'status'=>0,
      'QTime'=>1,
      'params'=>{
      'wt'=>'ruby',
      'rows'=>'10',
      'explainOther'=>'',
      'start'=>'0',
      'hl.fl'=>'',
      'indent'=>'on',
      'q'=>'guido',
      'fl'=>'*,score',
      'qt'=>'standard',
      'version'=>'2.2'}},
     'response'=>{'numFound'=>1,'start'=>0,'maxScore'=>0.67833745,'docs'=>[
      {
       'name'=>'guido von rossum',
       'id'=>'123',
       'timestamp'=>'2007-01-16T09:55:30.589Z',
       'score'=>0.67833745}]
     }}
RUBY_CODE
    conn = Solr::Connection.new 'http://localhost:9999'
    set_post_return(ruby_code)
    response = conn.send(Solr::Request::Standard.new(:query => 'foo'))
    assert_equal true, response.ok?
    assert response.query_time
    assert_equal 1, response.total_hits
    assert_equal 0, response.start
    assert_equal 0.67833745, response.max_score
    assert_equal 1, response.hits.length
  end

  def test_iteration
    ruby_code = 
<<RUBY_CODE
{
     'responseHeader'=>{
      'status'=>0,
      'QTime'=>0,
      'params'=>{
      'wt'=>'ruby',
      'rows'=>'10',
      'explainOther'=>'',
      'start'=>'0',
      'hl.fl'=>'',
      'indent'=>'on',
      'q'=>'guido',
      'fl'=>'*,score',
      'qt'=>'standard',
      'version'=>'2.2'}},
     'response'=>{'numFound'=>22,'start'=>0,'maxScore'=>0.53799295,'docs'=>[
      {
       'name'=>'guido von rossum the 0',
       'id'=>'0',
       'score'=>0.53799295},
      {
       'name'=>'guido von rossum the 1',
       'id'=>'1',
       'score'=>0.53799295},
      {
       'name'=>'guido von rossum the 2',
       'id'=>'2',
       'score'=>0.53799295},
      {
       'name'=>'guido von rossum the 3',
       'id'=>'3',
       'score'=>0.53799295},
      {
       'name'=>'guido von rossum the 4',
       'id'=>'4',
       'score'=>0.53799295},
      {
       'name'=>'guido von rossum the 5',
       'id'=>'5',
       'score'=>0.53799295},
      {
       'name'=>'guido von rossum the 6',
       'id'=>'6',
       'score'=>0.53799295},
      {
       'name'=>'guido von rossum the 7',
       'id'=>'7',
       'score'=>0.53799295},
      {
       'name'=>'guido von rossum the 8',
       'id'=>'8',
       'score'=>0.53799295},
      {
       'name'=>'guido von rossum the 9',
       'id'=>'9',
       'score'=>0.53799295}]
     }}
RUBY_CODE
    conn = Solr::Connection.new 'http://localhost:9999'
    set_post_return(ruby_code)

    count = 0
    conn.query('foo') do |hit|
      assert_equal "guido von rossum the #{count}", hit['name']
      count += 1
    end

    assert_equal 10, count
  end
  
  def test_facets
    ruby_code =
    <<RUBY_CODE
    {
     'responseHeader'=>{
      'status'=>0,
      'QTime'=>1897,
      'params'=>{
    	'facet.limit'=>'20',
    	'wt'=>'ruby',
    	'rows'=>'0',
    	'facet'=>'true',
    	'facet.mincount'=>'1',
    	'facet.field'=>[
    	 'subject_genre_facet',
    	 'subject_geographic_facet',
    	 'subject_format_facet',
    	 'subject_era_facet',
    	 'subject_topic_facet'],
    	'indent'=>'true',
    	'fl'=>'*,score',
    	'q'=>'[* TO *]',
    	'qt'=>'standard',
    	'facet.sort'=>'true'}},
     'response'=>{'numFound'=>49999,'start'=>0,'maxScore'=>1.0,'docs'=>[]
     },
     'facet_counts'=>{
      'facet_queries'=>{},
      'facet_fields'=>{
    	'subject_genre_facet'=>[
    	  'Biography.',2605,
    	 'Congresses.',1837,
    	 'Bibliography.',672,
    	 'Exhibitions.',642,
    	 'Periodicals.',615,
    	 'Sources.',485]}}
  	 }
RUBY_CODE
    set_post_return(ruby_code)
    conn = Solr::Connection.new "http://localhost:9999"
    response = conn.query('foo')
    facets = response.field_facets('subject_genre_facet')
    assert_equal 2605, facets[0].value
    assert_equal 485, facets[5].value
  end

end

