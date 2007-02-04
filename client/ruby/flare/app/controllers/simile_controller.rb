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


class SimileController < ApplicationController
  def exhibit
    @info = SOLR.send(Solr::Request::IndexInfo.new) # TODO move this call to only have it called when the index may have changed
    @facet_fields = @info.field_names.find_all {|v| v =~ /_facet$/}
    
    # TODO Add paging and sorting
    req = Solr::Request::Standard.new :query => query,
                                          :filter_queries => filters,
                                          :facets => {:fields => @facet_fields, :limit => 20 , :mincount => 1, :sort => :count, :debug_query=>true}
    @data = SOLR.send(req)
    @data.each {|d| d['label'] = d['title_text']}
    
    respond_to do |format| 
      puts "format = #{format.inspect}"
      format.html # renders index.rhtml 
      format.json { render :json => {'items' => @data}.to_json } 
    end                                         
  end
end
