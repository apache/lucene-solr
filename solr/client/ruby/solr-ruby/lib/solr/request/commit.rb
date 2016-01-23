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

class Solr::Request::Commit < Solr::Request::Update

  def initialize(options={})
    @wait_searcher = options[:wait_searcher] || true
    @wait_flush = options[:wait_flush] || true
  end


  def to_s
    e = Solr::XML::Element.new('commit')
    e.attributes['waitSearcher'] = @wait_searcher ? 'true' : 'false'
    e.attributes['waitFlush'] = @wait_flush ? 'true' : 'false'
    
    e.to_s
  end

end
