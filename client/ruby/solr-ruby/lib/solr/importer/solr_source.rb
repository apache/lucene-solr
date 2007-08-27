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

require 'solr'

class Solr::Importer::SolrSource
  def initialize(solr_url, query, filter_queries=nil, options={})
    @connection = Solr::Connection.new(solr_url)
    @query = query
    @filter_queries = filter_queries

    @page_size = options[:page_size] || 1000
    @field_list = options[:field_list] || ["*"]
  end
  
  def each
    done = false
    start = 0
    until done do
      # request N documents from a starting point
      request = Solr::Request::Standard.new(:query => @query,
                                            :rows => @page_size,
                                            :start => start,
                                            :field_list => @field_list,
                                            :filter_queries => @filter_queries)
      response = @connection.send(request)
      response.each do |doc|
        yield doc  # TODO: perhaps convert to HashWithIndifferentAccess.new(doc), so stringify_keys isn't necessary
      end
      done = start + @page_size >= response.total_hits
      start = start + @page_size
    end
  end
end
