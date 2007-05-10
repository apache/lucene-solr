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

require 'net/http'

# TODO: add a convenience method to POST a Solr .xml file, like Solr's example post.sh

class Solr::Connection
  attr_reader :url, :autocommit, :connection

  # create a connection to a solr instance using the url for the solr
  # application context:
  #
  #   conn = Solr::Connection.new("http://example.com:8080/solr")
  #
  # if you would prefer to have all adds/updates autocommitted, 
  # use :autocommit => :on
  #
  #   conn = Solr::Connection.new('http://example.com:8080/solr', 
  #     :autocommit => :on)

  def initialize(url, opts={})
    @url = URI.parse(url)
    unless @url.kind_of? URI::HTTP
      raise "invalid http url: #{url}"
    end
  
    # TODO: Autocommit seems nice at one level, but it currently is confusing because
    # only calls to Connection#add/#update/#delete, though a Connection#send(AddDocument.new(...))
    # does not autocommit.  Maybe #send should check for the request types that require a commit and
    # commit in #send instead of the individual methods?
    @autocommit = opts[:autocommit] == :on
  
    # Not actually opening the connection yet, just setting up the persistent connection.
    @connection = Net::HTTP.new(@url.host, @url.port)
    
    @connection.read_timeout = opts[:timeout] if opts[:timeout]
  end

  # add a document to the index. you can pass in either a hash
  #
  #   conn.add(:id => 123, :title => 'Tlon, Uqbar, Orbis Tertius')
  #
  # or a Solr::Document
  #
  #   conn.add(Solr::Document.new(:id => 123, :title = 'On Writing')
  #
  # true/false will be returned to designate success/failure

  def add(doc)
    request = Solr::Request::AddDocument.new(doc)
    response = send(request)
    commit if @autocommit
    return response.ok?
  end

  # update a document in the index (really just an alias to add)

  def update(doc)
    return add(doc)
  end

  # performs a standard query and returns a Solr::Response::Standard
  #
  #   response = conn.query('borges')
  # 
  # alternative you can pass in a block and iterate over hits
  #
  #   conn.query('borges') do |hit|
  #     puts hit
  #   end

  def query(query, options={}, &action)
    # TODO: Shouldn't this return an exception if the Solr status is not ok?  (rather than true/false).
    create_and_send_query(Solr::Request::Standard, options.update(:query => query), &action)
  end
  
  def search(query, options={}, &action)
    create_and_send_query(Solr::Request::Dismax, options.update(:query => query), &action)
  end

  # sends a commit message to the server
  def commit(options={})
    response = send(Solr::Request::Commit.new(options))
    return response.ok?
  end

  # sends an optimize message to the server
  def optimize
    response = send(Solr::Request::Optimize.new)
    return response.ok?
  end
  
  # pings the connection and returns true/false if it is alive or not
  def ping
    begin
      response = send(Solr::Request::Ping.new)
      return response.ok?
    rescue
      return false
    end
  end

  # delete a document from the index using the document id
  def delete(document_id)
    response = send(Solr::Request::Delete.new(:id => document_id))
    commit if @autocommit
    response.ok?
  end

  # delete using a query
  def delete_by_query(query)
    response = send(Solr::Request::Delete.new(:query => query))
    commit if @autocommit
    response.ok?
  end
  
  def info
    send(Solr::Request::IndexInfo.new)
  end
  
  # send a given Solr::Request and return a RubyResponse or XmlResponse
  # depending on the type of request
  def send(request)
    data = post(request)
    Solr::Response::Base.make_response(request, data)
  end

  # send the http post request to solr; for convenience there are shortcuts
  # to some requests: add(), query(), commit(), delete() or send()
  def post(request)
    response = @connection.post(@url.path + "/" + request.handler,
                                request.to_s,
                                { "Content-Type" => request.content_type })
  
    case response
    when Net::HTTPSuccess then response.body
    else
      response.error!
    end
  
  end
  
private
  
  def create_and_send_query(klass, options = {}, &action)
    request = klass.new(options)
    response = send(request)
    return response unless action
    response.each {|hit| action.call(hit)}
  end
  
end
