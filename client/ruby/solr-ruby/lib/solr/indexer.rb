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

class Solr::Indexer
  def self.index(data_source, mapper, options={})
    solr_url = options[:solr_url] || ENV["SOLR_URL"] || "http://localhost:8983/solr"
    
    solr = Solr::Connection.new(solr_url, options) #TODO - these options contain the solr_url and debug keys also, so tidy up what gets passed
    data_source.each do |record|
      document = mapper.map(record)
      
      yield(record, document) if block_given?
      
      solr.add(document) unless options[:debug]
      puts document.inspect if options[:debug]
    end
    solr.commit unless options[:debug]
  end
end