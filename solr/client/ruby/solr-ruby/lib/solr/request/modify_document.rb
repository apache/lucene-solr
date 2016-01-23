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
    @doc = {}
    [:overwrite, :append, :distinct, :increment, :delete].each do |mode|
      field_data = update_data[mode]
      if field_data
        field_data.each do |field_name, field_value|
          modes << "#{field_name}:#{mode.to_s.upcase}"
          @doc[field_name] = field_value if field_value  # if value is nil, omit so it can be removed
        end
        update_data.delete mode
      end
    end
    @mode = modes.join(",")
    
    # only one key should be left over, the id
    @doc[update_data.keys[0].to_s] = update_data.values[0]
  end

  # returns the request as a string suitable for posting
  def to_s
    e = Solr::XML::Element.new 'add'
    e.add_element(Solr::Document.new(@doc).to_xml)
    return e.to_s
  end
  
  def handler
    "update?mode=#{@mode}"
  end
  
end
