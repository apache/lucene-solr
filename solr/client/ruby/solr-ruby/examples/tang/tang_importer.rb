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

require 'hpricot'
require 'solr'

solr_url = ENV["SOLR_URL"] || "http://localhost:8983/solr"
debug = ARGV[1] == "-debug"

solr = Solr::Connection.new(solr_url)

html = Hpricot(open(ARGV[0]))
max = 320

def next_blockquote(elem)
  elem = elem.next_sibling
  until elem.name == "blockquote" do
    elem = elem.next_sibling
  end
  
  elem
end

for current_index in (1..max) do
  section_start = html.at("//blockquote[text()='#{format('%03d',current_index)}']")
  type_zh = next_blockquote(section_start)
  author_zh = next_blockquote(type_zh)
  title_zh = next_blockquote(author_zh)
  body_zh = next_blockquote(title_zh)
  
  type_en = next_blockquote(body_zh)
  author_en = next_blockquote(type_en)
  title_en = next_blockquote(author_en)
  body_en = next_blockquote(title_en)
  doc = {:type_zh_facet => type_zh, :author_zh_facet => author_zh, :title_zh_text => title_zh, :body_zh_text => body_zh,
         :type_en_facet => type_en, :author_en_facet => author_en, :title_en_text => title_en, :body_en_text => body_en
        }
  doc.each {|k,v| doc[k] = v.inner_text}
  doc[:id] = current_index    # TODO: namespace the id, something like "etext_tang:#{current_index}"
  doc[:source_facet] = 'etext_tang'
  doc[:language_facet] = ['chi','eng']

  puts "----",doc[:id],doc[:title_en_text],doc[:author_en_facet],doc[:type_en_facet]
#  puts doc.inspect if debug
  solr.add doc unless debug
end

solr.commit unless debug
#solr.optimize unless debug