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

class Solr::Request::Spellcheck < Solr::Request::Select

  def initialize(params)
    super('spellchecker')
    @params = params
  end
  
  def to_hash
    hash = super
    hash[:q] = @params[:query]
    hash[:suggestionCount] = @params[:suggestion_count]
    hash[:accuracy] = @params[:accuracy]
    hash[:onlyMorePopular] = @params[:only_more_popular]
    hash[:cmd] = @params[:command]
    return hash
  end

end