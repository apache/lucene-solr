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

begin
  require 'xml/libxml'

  # For files with the first line containing field names
  class Solr::Importer::XPathMapper < Solr::Importer::Mapper
    def field_data(doc, xpath)
      doc.find(xpath.to_s).collect do |node|
        case node
          when XML::Attr
            node.value
          when XML::Node
            node.content
        end
      end
    end
  end
rescue LoadError => e # If we can't load libxml
  class Solr::Importer::XPathMapper
    def initialize(mapping, options={})
      raise "libxml not installed"
    end
  end
end
