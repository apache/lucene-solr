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

end

