require 'solr_mock_base'

class DeleteTest <  SolrMockBaseTestCase

  def test_delete_request
    request = Solr::Request::Delete.new(:id => '123')
    assert_equal "<delete><id>123</id></delete>", request.to_s
  end

  def test_delete_by_query_request
    request = Solr::Request::Delete.new(:query => 'name:summers')
    assert_equal "<delete><query>name:summers</query></delete>", request.to_s
  end

  def test_delete_response
    conn = Solr::Connection.new 'http://localhost:9999/solr'
    set_post_return('<result status="0"></result>')
    response = conn.send(Solr::Request::Delete.new(:id => 123))
    assert_equal true, response.ok? 
  end

  def test_bad_delete_response
    conn = Solr::Connection.new 'http://localhost:9999/solr'
    set_post_return('<result status="400">uhoh</result>')
    response = conn.send(Solr::Request::Delete.new(:id => 123))
    assert_equal false, response.ok? 
  end

end
