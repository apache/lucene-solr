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

require 'rexml/document'

class Solr::Field
  attr_accessor :name
  attr_accessor :value

  def initialize(key_val, opts={})
    raise "first argument must be a hash" unless key_val.kind_of? Hash
    @name = key_val.keys[0].to_s
    @value = key_val.values[0].to_s
  end

  def to_xml
    e = REXML::Element.new 'field'
    e.attributes['name'] = @name
    e.text = @value
    return e
  end

end
