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
  attr_reader :solr
  
  # TODO: document options!
  def initialize(data_source, mapper_or_mapping, options={})
    solr_url = options[:solr_url] || ENV["SOLR_URL"] || "http://localhost:8983/solr"
    @solr = Solr::Connection.new(solr_url, options) #TODO - these options contain the solr_url and debug keys also, so tidy up what gets passed

    @data_source = data_source
    @mapper = mapper_or_mapping.is_a?(Hash) ? Solr::Importer::Mapper.new(mapper_or_mapping) : mapper_or_mapping

    @buffer_docs = options[:buffer_docs]
    @debug = options[:debug]
  end

  def index
    buffer = []
    @data_source.each do |record|
      document = @mapper.map(record)
      
      # TODO: check arrity of block, if 3, pass counter as 3rd argument
      yield(record, document) if block_given? # TODO check return of block, if not true then don't index, or perhaps if document.empty?
      
      buffer << document
      
      if !@buffer_docs || buffer.size == @buffer_docs
        add_docs(buffer)
        buffer.clear
      end
    end
    add_docs(buffer) if !buffer.empty?
    
    @solr.commit unless @debug
  end
  
  def add_docs(documents)
    @solr.add(documents) unless @debug
    puts documents.inspect if @debug
  end
end
