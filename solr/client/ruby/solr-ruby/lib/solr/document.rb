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
require 'solr/field'

class Solr::Document
  include Enumerable
  attr_accessor :boost

  # Create a new Solr::Document, optionally passing in a hash of 
  # key/value pairs for the fields
  #
  #   doc = Solr::Document.new(:creator => 'Jorge Luis Borges')
  def initialize(hash={})
    @fields = []
    self << hash
  end

  # Append a Solr::Field
  #
  #   doc << Solr::Field.new(:creator => 'Jorge Luis Borges')
  #
  # If you are truly lazy you can simply pass in a hash:
  #
  #   doc << {:creator => 'Jorge Luis Borges'}
  def <<(fields)
    case fields
    when Hash
      fields.each_pair do |name,value|
        if value.respond_to?(:each) && !value.is_a?(String)
          value.each {|v| @fields << Solr::Field.new(name => v)}
        else
          @fields << Solr::Field.new(name => value)
        end
      end
    when Solr::Field
      @fields << fields
    else
      raise "must pass in Solr::Field or Hash"
    end
  end

  # shorthand to allow hash lookups
  #   doc['name']
  def [](name)
    field = @fields.find {|f| f.name == name.to_s}
    return field.value if field
    return nil
  end

  # shorthand to assign as a hash
  def []=(name,value)
    @fields << Solr::Field.new(name => value)
  end

  # convert the Document to a REXML::Element 
  def to_xml
    e = Solr::XML::Element.new 'doc'
    e.attributes['boost'] = @boost.to_s if @boost
    @fields.each {|f| e.add_element(f.to_xml)}
    return e
  end
end
