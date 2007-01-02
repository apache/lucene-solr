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
include REXML

module Solr
  class Connection
    attr_reader :url
    
    def initialize(url)
      @url = URI.parse(url)
    end

    def send(request)
      post = Net::HTTP::Post.new(request.url_path)
      post.body = request.to_http_body
      post.content_type = 'application/x-www-form-urlencoded; charset=utf-8'
      response = Net::HTTP.start(@url.host, @url.port) do |http|
        http.request(post)
      end
      return request.response_format == :ruby ? RubyResponse.new(response.body) : XmlResponse.new(response.body)
    end
  end
end
