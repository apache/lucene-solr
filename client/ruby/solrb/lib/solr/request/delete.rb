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

require 'solr/xml'

class Solr::Request::Delete < Solr::Request::Update

  # A delete request can be for a specific document id
  #
  #   request = Solr::Request::Delete.new(:id => 1234)
  #
  # or by query:
  #
  #   request = Solr::Request::Delete.new(:query =>
  #
  def initialize(options)
    unless options.kind_of?(Hash) and (options[:id] or options[:query])
      raise Solr::Exception.new("must pass in :id or :query")
    end
    if options[:id] and options[:query]
      raise Solr::Exception.new("can't pass in both :id and :query")
    end
    @document_id = options[:id]
    @query = options[:query]
  end

  def to_s
    delete_element = Solr::XML::Element.new('delete')
    if @document_id
      id_element = Solr::XML::Element.new('id')
      id_element.text = @document_id
      delete_element.add_element(id_element)
    elsif @query
      query = Solr::XML::Element.new('query')
      query.text = @query 
      delete_element.add_element(query)
    end
    delete_element.to_s
  end
end

