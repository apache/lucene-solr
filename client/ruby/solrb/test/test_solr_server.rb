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

# A singleton class for starting/stopping a Solr server for testing purposes
# The behavior of TestSolrServer can be modified prior to start() by changing 
# port, solr_home, and quiet properties.
 
class TestSolrServer
  require 'singleton'
  include Singleton
  attr_accessor :port, :solr_home, :quiet
 
  # configure the singleton with some defaults
  def initialize
    @port = 8888
    @quiet = true
    root_dir = File.expand_path(File.dirname(__FILE__) + '/..')
    @solr_dir = "#{root_dir}/solr"
    @solr_home = "#{root_dir}/test" 
    @pid = nil
  end

  # start the solr server
  def start
    Dir.chdir(@solr_dir) do
      @pid = fork do
        STDERR.close if @quiet
        exec "java -Djetty.port=#{@port} -Dsolr.solr.home=#{@solr_home} " + 
          "-jar start.jar"
      end
    end
  end
 
  # stop a running solr server
  def stop
    Process.kill('TERM', @pid)
    Process.wait
  end
 end
