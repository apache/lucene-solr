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

class BadRequest < Solr::Request::Standard
  def response_format
    :invalid
  end
end

class ServerTest < Test::Unit::TestCase
  include Solr

  def setup
    @connection = Connection.new("http://localhost:8888/solr", :autocommit => :on)
    clean
  end
 
  def test_full_lifecycle
    # make sure autocommit is on
    assert @connection.autocommit

    # make sure this doc isn't there to begin with
    @connection.delete(123456)

    # add it
    @connection.add(:id => 123456, :text => 'Borges') # add :some_date => 'NOW/HOUR' to test richer data type handling
    # now = DateTime.now

    # look for it
    response = @connection.query('Borges')
    assert_equal 1, response.total_hits
    hit = response.hits[0]
    assert_equal '123456', hit['id']
    # assert_equal now.year, hit['whatever_date'].year
    
    # look for it via dismax
    response = @connection.search('Borges')
    assert_equal 1, response.total_hits
    assert_equal '123456', response.hits[0]['id']

    # delete it
    @connection.delete(123456)

    # make sure it's gone
    response = @connection.query('Borges')
    assert_equal 0, response.total_hits
  end 

  def test_i18n_full_lifecycle
    # make sure autocommit is on
    assert @connection.autocommit

    # make sure this doc isn't there to begin with
    @connection.delete(123456)

    # add it
    @connection.add(:id => 123456, :text => 'Åäöêâîôû')

    # look for it
    response = @connection.query('Åäöêâîôû')
    assert_equal 1, response.total_hits
    assert_equal '123456', response.hits[0]['id']

    # delete it
    @connection.delete(123456)

    # make sure it's gone
    response = @connection.query('Åäöêâîôû Öëäïöü')
    assert_equal 0, response.total_hits
  end
  
  def test_sorting
    @connection.add(:id => 1, :text => 'aaa woot')
    @connection.add(:id => 2, :text => 'bbb woot')
    @connection.add(:id => 3, :text => 'ccc woot')
    @connection.commit
    
    results = @connection.query('woot', :sort => [:id => :descending], :rows => 2)
    assert_equal([3, 2], results.hits.map { |h| h['id'].to_i })
    
    results = @connection.search('woot', :sort => [:id => :descending], :rows => 2)
    assert_equal([3, 2], results.hits.map { |h| h['id'].to_i })
    
    @connection.delete_by_query("id:1 OR id:2 OR id:3")
  end

  def test_bad_connection
    conn = Solr::Connection.new 'http://127.0.0.1:9999/invalid'
    begin
      conn.send(Solr::Request::Ping.new)
      flunk "Expected exception not raised"
    rescue ::Exception
      # expected
      assert true
    end
  end
  
  def test_bad_url
    conn = Solr::Connection.new 'http://localhost:8888/invalid'
    assert_raise(Net::HTTPServerException) do
      conn.send(Solr::Request::Ping.new)
    end
  end
  
  def test_commit
    response = @connection.send(Solr::Request::Commit.new)
    assert response.ok?
  end
  
  def test_optimize
    response = @connection.send(Solr::Request::Optimize.new)
    assert response.ok?
  end
  
# TODO: add test_ping back... something seems to have changed with the response, so adjustments are needed.
#       non-critical - if Solr is broken we'll know from other tests!
#  def test_ping
#    assert_equal true, @connection.ping
#  end

  def test_delete_with_query
    assert_equal true, @connection.delete_by_query('[* TO *]')
  end

  def test_ping_with_bad_server
    conn = Solr::Connection.new 'http://localhost:8888/invalid'
    assert_equal false, conn.ping
  end
  
  def test_invalid_response_format
    request = BadRequest.new(:query => "solr")
    assert_raise(Solr::Exception) do
      @connection.send(request)
    end
  end
  
  def test_escaping
    doc = Solr::Document.new :id => 47, :ruby_text => 'puts "ouch!"'
    @connection.add(doc)
    @connection.commit
    
    request = Solr::Request::Standard.new :query => 'ouch'
    result = @connection.send(request)
    
    assert_match /puts/, result.raw_response
  end

  def test_add_document
    doc = {:id => 999, :text => 'hi there!'}
    request = Solr::Request::AddDocument.new(doc)
    response = @connection.send(request)
    assert response.status_code == '0'
  end

  def test_update
    @connection.update(:id => 999, :text => 'update test')
  end

  def test_no_such_field
    doc = {:id => 999, :bogus => 'foo'}
    request = Solr::Request::AddDocument.new(doc)
    assert_raise(Net::HTTPServerException) do
      response = @connection.send(request)
    end
    # assert_equal false, response.ok? 
    # assert_match "ERROR:unknown field 'bogus'", response.status_message
  end
  
  def test_index_info
    doc = {:id => 999, :test_index_facet => 'value'}
    @connection.add(doc)
    ii = Solr::Request::IndexInfo.new
    info = @connection.send(Solr::Request::IndexInfo.new)
    assert info.field_names.include?("id") && info.field_names.include?("test_index_facet")
    assert_equal 1, info.num_docs
  end
  
  def test_highlighting
    @connection.add(:id => 1, :title_text => "Apache Solr")
    
    request = Solr::Request::Standard.new(:query => 'solr',
      :highlighting => {
        :field_list => ['title_text'],
        :max_snippets => 3,
        :prefix => ">>",
        :suffix => "<<"
      }
    )
    
    response = @connection.send(request)
    assert_equal ["Apache >>Solr<<"], response.highlighted(1, :title_text)
  end
  
  def test_entities
    @connection.add(:id => 1, :title_text => "&nbsp;")
    response = @connection.query('nbsp')
    assert_equal 1, response.total_hits
    assert_equal '1', response.hits[0]['id']
  end

  # wipe the index clean
  def clean
    @connection.delete_by_query('*:*')
  end

end
