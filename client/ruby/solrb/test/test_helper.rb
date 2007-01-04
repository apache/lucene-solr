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

def start_solr_server
  Dir.chdir(File.dirname(__FILE__) + '/../solr') do 
    puts "starting solr server"

    # start solr and capture the process ID in a global
    $SOLR_PID = fork do

      # don't want to see the messages about solr starting up
#      STDERR.close 

      exec "java -Djetty.port=8888 -Dsolr.solr.home=../test -jar start.jar"
    end
  end

  # wait for the jvm and solr to start
  sleep 10 
end

def stop_solr_server
  puts "stopping solr server"
  Process.kill('TERM', $SOLR_PID)
end

