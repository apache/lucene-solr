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

module Solr
  class Connection
    attr_reader :url, :autocommit
   
    # create a connection to a solr instance using the url for the solr
    # application context:
    #
    #   conn = Solr::Connection.new("http://example.com:8080/solr")
    #
    # if you would prefer to issue your own commits to cut down on 
    # network traffic use :autocommit => 'off'
    #
    #   conn = Solr::Connection.new('http://example.com:8080/solr', 
    #     :autocommit => 'off')
    
    def initialize(url, opts={})
      @url = URI.parse(url)
      unless @url.kind_of? URI::HTTP
        raise "invalid http url: #{url}"
      end
      @autocommit = opts[:autocommit] == :on ? true : false
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
      doc = Solr::Document.new(doc)
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
      options[:query] = query
      request = Solr::Request::Standard.new(options)
      response = send(request)
      return response unless action
      response.each {|hit| action.call(hit)}
    end

    # sends a commit message to the server
    def commit
      response = send(Solr::Request::Commit.new)
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
      return response.ok?
    end

    # delete using a query
    def delete_by_query(query)
      response = send(Solr::Request::Delete.new(:query => query))
      commit if @autocommit
      return response.ok?
    end

    # send a given Solr::Request and return a RubyResponse or XmlResponse
    # depending on the type of request
    def send(request)
      data = post(request)
      return Solr::Response::Base.make_response(request, data)
    end
   
    # send the http post request to solr: you will want to use
    # one of the add(), query(), commit(), delete() or send()
    # instead of this...
    def post(request)
      post = Net::HTTP::Post.new(@url.path + "/" + request.handler)
      post.body = request.to_s
      post.content_type = 'application/x-www-form-urlencoded; charset=utf-8'
      response = Net::HTTP.start(@url.host, @url.port) do |http|
        http.request(post)
      end
      
      case response
      when Net::HTTPSuccess then response.body
      else
        response.error!
      end
      
    end
  end
end
