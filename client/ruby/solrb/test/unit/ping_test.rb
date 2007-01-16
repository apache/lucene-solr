require 'solr_mock_base'

class PingTest < SolrMockBaseTestCase 

  def test_ping_response
    xml = 
<<PING_RESPONSE

<?xml-stylesheet type="text/xsl" href="ping.xsl"?>

<solr>
  <ping>

  </ping>
</solr>
PING_RESPONSE
    conn = Solr::Connection.new('http://localhost:9999')
    set_post_return(xml)
    response = conn.send(Solr::Request::Ping.new)
    assert_kind_of Solr::Response::Ping, response
    assert_equal true, response.ok? 

    # test shorthand
    assert true, conn.ping
  end

  def test_bad_ping_response
    xml = "<foo>bar</foo>"
    conn = Solr::Connection.new('http://localhost:9999')
    set_post_return(xml)
    response = conn.send(Solr::Request::Ping.new)
    assert_kind_of Solr::Response::Ping, response
    assert_equal false, response.ok?

    # test shorthand
    assert_equal false, conn.ping
  end

end
