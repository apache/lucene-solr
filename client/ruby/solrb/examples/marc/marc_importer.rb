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

require 'marc'
require 'solr'

solr_url = ENV["SOLR_URL"] || "http://localhost:8983/solr"
marc_filename = ARGV[0]

mapping = {
  :id => Proc.new {|r| r['001'].value},
  :subject_genre_facet => ['650v', '655a'],
  :subject_era_facet => '650y',
  :title_text => '245a'
}

connection = Solr::Connection.new(solr_url)

reader = MARC::Reader.new(marc_filename)
count = 0
for record in reader
  doc = {}
  mapping.each do |key,value|
    if value.kind_of? Proc
      doc[key] = value.call(record)
    else
      data = []
      value.each do |v|
        field = v[0,3]
        subfield = v[3].chr
        data << record[field][subfield] rescue nil
      end
      data.compact!
      doc[key] = data if not data.empty?
    end
  end

  connection.send(Solr::Request::AddDocument.new(doc))
  
  count += 1
  
  puts count if count % 100 == 0
end

connection.send(Solr::Request::Commit.new)
