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

# Borrowed from <http://pastie.caboo.se/23100>, by Josh Susser
desc "Print out all the currently defined routes, with names."
task :routes => :environment do
  name_col_width = ActionController::Routing::Routes.named_routes.routes.keys.sort {|a,b| a.to_s.size <=> b.to_s.size}.last.to_s.size
  ActionController::Routing::Routes.routes.each do |route|
    name = ActionController::Routing::Routes.named_routes.routes.index(route).to_s
    name = name.ljust(name_col_width + 1)
    puts "#{name}#{route}"
  end
end