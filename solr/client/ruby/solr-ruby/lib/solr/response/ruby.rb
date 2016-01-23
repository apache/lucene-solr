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

class Solr::Response::Ruby < Solr::Response::Base
  attr_reader :data, :header

  def initialize(ruby_code)
    super
    begin
      #TODO: what about pulling up data/header/response to ResponseBase,
      #      or maybe a new middle class like SelectResponseBase since
      #      all Select queries return this same sort of stuff??
      #      XML (&wt=xml) and Ruby (&wt=ruby) responses contain exactly the same structure.
      #      a goal of solrb is to make it irrelevant which gets used under the hood, 
      #      but favor Ruby responses.
      @data = eval(ruby_code)
      @header = @data['responseHeader']
      raise "response should be a hash" unless @data.kind_of? Hash
      raise "response header missing" unless @header.kind_of? Hash
    rescue SyntaxError => e
      raise Solr::Exception.new("invalid ruby code: #{e}")
    end
  end

  def ok?
    @header['status'] == 0
  end

  def query_time
    @header['QTime']
  end
  
end
