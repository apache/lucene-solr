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

class Solr::Response::Dismax < Solr::Response::Standard
  # no need for special processing
  
  # FIXME: 2007-02-07 <coda.hale@gmail.com> --  The existence of this class indicates that
  # the Request/Response pair architecture is a little hinky. Perhaps we could refactor
  # out some of the most common functionality -- Common Query Parameters, Highlighting Parameters,
  # Simple Facet Parameters, etc. -- into modules?
end