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
    attr_reader :url
    
    def initialize(url)
      @url = URI.parse(url)
      unless @url.kind_of? URI::HTTP
        raise "invalid http url: #{url}"
      end
    end

    # sends a commit message
    def commit
      self.send(Solr::Request::Commit.new)
    end

    # sends a ping message
    def ping
      response = send(Solr::Request::Ping.new)
    end

    def send(request)
      data = post(request)
      case request.response_format
      when :ruby
        return RubyResponse.new(data)
      when :xml
        return XmlResponse.new(data)
      else
        raise "Unknown response format: #{request.response_format}"
      end
    end
    
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
