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
require 'solr/request/base'
require 'solr/document'
require 'solr/request/update'

class Solr::Request::AddDocument < Solr::Request::Update

  # create the request, optionally passing in a Solr::Document
  #
  #   request = Solr::Request::AddDocument.new doc
  #
  # as a short cut you can pass in a Hash instead:
  #
  #   request = Solr::Request::AddDocument.new :creator => 'Jorge Luis Borges'
  # 
  # or an array, to add multiple documents at the same time:
  # 
  #   request = Solr::Request::AddDocument.new([doc1, doc2, doc3])
    
  def initialize(doc={})
    @docs = []
    if doc.is_a?(Array)
      doc.each { |d| add_doc(d) }
    else
      add_doc(doc)
    end
  end

  # returns the request as a string suitable for posting
  
  def to_s
    e = Solr::XML::Element.new 'add'
    for doc in @docs
      e.add_element doc.to_xml
    end
    return e.to_s
  end
  
  private
  def add_doc(doc)
    case doc
    when Hash
      @docs << Solr::Document.new(doc)
    when Solr::Document
      @docs << doc
    else
      raise "must pass in Solr::Document or Hash"
    end
  end
  
end
