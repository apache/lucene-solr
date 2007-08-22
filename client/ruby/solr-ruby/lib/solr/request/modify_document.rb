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

class Solr::Request::ModifyDocument < Solr::Request::Update

  # Example: ModifyDocument.new(:id => 10, :overwrite => {:field_name => "new value"})
  def initialize(update_data)
    modes = []
    @fields = {}
    [:overwrite, :append, :distinct, :increment].each do |mode|
      field_data = update_data[mode]
      if field_data
        field_data.each do |field_name, field_value|
          modes << "#{field_name}:#{mode.to_s.upcase}"
          @fields[field_name] = field_value
        end
        update_data.delete mode
      end
    end
    @mode = modes.join(",")
    @id = update_data  # should only be one key remaining
  end

  # returns the request as a string suitable for posting
  def to_s
    e = Solr::XML::Element.new 'add'
    doc = Solr::XML::Element.new 'doc'
    e.add_element doc
    f = Solr::XML::Element.new 'field'
    f.attributes['name'] = @id.keys[0].to_s
    f.text = @id.values[0]
    doc.add_element f
    @fields.each do |key, value|
      f = Solr::XML::Element.new 'field'
      f.attributes['name'] = key.to_s
      # TODO - what about boost?  - can it be updated too?
      f.text = value
      doc.add_element f
    end
    return e.to_s
  end
  
  def handler
    "update?mode=#{@mode}"
  end
  
end
