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

module Solr::XML
end

begin
  
  # If we can load rubygems and libxml-ruby...
  require 'rubygems'
  require 'xml/libxml'
  
  # then make a few modifications to XML::Node so it can stand in for REXML::Element
  class XML::Node
    # element.add_element(another_element) should work
    alias_method :add_element, :<<

    # element.attributes['blah'] should work
    def attributes
      self
    end

    # element.text = "blah" should work
    def text=(x)
      self << x.to_s
    end
  end
  
  # And use XML::Node for our XML generation
  Solr::XML::Element = XML::Node
  
rescue LoadError => e # If we can't load either rubygems or libxml-ruby
  
  # Just use REXML.
  require 'rexml/document'
  Solr::XML::Element = REXML::Element
  
end