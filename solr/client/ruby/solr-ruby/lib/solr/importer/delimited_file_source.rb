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

# For files with the first line containing field names
# Currently not designed for enormous files, as all lines are
# read into an array
class Solr::Importer::DelimitedFileSource
  include Enumerable
  
  def initialize(filename, splitter=/\t/)
    @filename = filename
    @splitter = splitter
  end

  def each
    lines = IO.readlines(@filename)
    headers = lines[0].split(@splitter).collect{|h| h.chomp}
    
    lines[1..-1].each do |line|
      data = headers.zip(line.split(@splitter).collect{|s| s.chomp})
      def data.[](key)
        self.assoc(key.to_s)[1]
      end
      
      yield(data)
    end
  end
  
end
