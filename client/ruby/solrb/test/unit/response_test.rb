class ResponseTest < Test::Unit::TestCase

  def test_response_error
    assert_raise(Solr::RequestException) do
      new Solr::Response.new("<result status=\"400\">ERROR:</result>")
    end
    
    begin
      new Solr::Response.new("<result status=\"400\">ERROR:</result>")
    rescue Solr::RequestException => exception
      assert_equal "ERROR:", exception.message
      assert_equal exception.message, exception.to_s
      assert_equal "400", exception.code
    end
  end

end
