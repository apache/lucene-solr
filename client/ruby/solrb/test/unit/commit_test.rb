require 'solr_mock_base'

class CommitTest < SolrMockBaseTestCase

  def test_commit
    xml = '<result status="0"></result>'
    conn = Solr::Connection.new('http://localhost:9999/solr')
    set_post_return(xml)
    response = conn.send(Solr::Request::Commit.new)
    assert_kind_of Solr::Response::Commit, response
    assert true, response.ok?

    # test shorthand
    assert_equal true, conn.commit
  end

  def test_invalid_commit
    xml = '<foo>bar</foo>'
    conn = Solr::Connection.new('http://localhost:9999/solr')
    set_post_return(xml)
    response = conn.send(Solr::Request::Commit.new)
    assert_kind_of Solr::Response::Commit, response
    assert_equal false, response.ok?

    # test shorthand
    assert_equal false, conn.commit
   end

end
