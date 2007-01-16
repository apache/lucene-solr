require 'solr_mock_base'

class AddDocumentTest < SolrMockBaseTestCase

  def test_add_document_response
    conn = Solr::Connection.new('http://localhost:9999/solr')
    set_post_return('<result status="0"></result>')
    doc = {:id => '123', :text => 'Tlon, Uqbar, Orbis Tertius'}
    response = conn.send(Solr::Request::AddDocument.new(doc))
    assert_equal true, response.ok?
  end

  def test_bad_add_document_response
    conn = Solr::Connection.new('http://localhost:9999/solr')
    set_post_return('<result status="400"></result>')
    doc = {:id => '123', :text => 'Tlon, Uqbar, Orbis Tertius'}
    response = conn.send(Solr::Request::AddDocument.new(doc))
    assert_equal false, response.ok?
  end

  def test_shorthand
    conn = Solr::Connection.new('http://localhost:9999/solr')
    set_post_return('<result status="0"></result>')
    doc = {:id => '123', :text => 'Tlon, Uqbar, Orbis Tertius'}
    assert_equal true, conn.add(:id => '123', :text => 'Tlon, Uqbar, Orbis Tetius')
  end

end
