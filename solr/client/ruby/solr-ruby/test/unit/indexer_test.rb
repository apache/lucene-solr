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

class Solr::Indexer
  attr_reader :added
  def add_docs(doc)
    @added ||= []
    @added << doc
  end
end

class IndexerTest < Test::Unit::TestCase
  def test_mapping_or_mapping
    mapping = {:field => "foo"}
    indexer = Solr::Indexer.new([1,2,3], mapping, :debug => true)
    indexer.index
    assert_equal 3, indexer.added.size
    
    indexer = Solr::Indexer.new([1,2,3,4], Solr::Importer::Mapper.new(mapping), :debug => true)
    indexer.index
    assert_equal 4, indexer.added.size
  end

  def test_batch
    mapping = {:field => "foo"}
    indexer = Solr::Indexer.new([1,2,3], mapping, :debug => true, :buffer_docs => 2)
    indexer.index
    assert_equal 2, indexer.added.size
  end
  
end


# source = DataSource.new
# 
# mapping = {
#   :id => :isbn,
#   :name => :author,
#   :source => "BOOKS",
#   :year => Proc.new {|record| record.date[0,4] },
# }
# 
# Solr::Indexer.index(source, mapper) do |orig_data, solr_document|
#   solr_document[:timestamp] = Time.now
# end