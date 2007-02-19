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
  attr_accessor :port, :jetty_home, :solr_home, :quiet

  # configure the singleton with some defaults
  def initialize
    @pid = nil
  end

  def self.wrap(params = {})
    error = false
    solr_server = self.instance
    solr_server.quiet = params[:quiet] || true
    solr_server.jetty_home = params[:jetty_home]
    solr_server.solr_home = params[:solr_home]
    solr_server.port = params[:jetty_port] || 8888
    begin
      puts "starting solr server on #{RUBY_PLATFORM}"
      solr_server.start
      sleep params[:startup_wait] || 5
      yield
    rescue
      error = true
    ensure
      puts "stopping solr server"
      solr_server.stop
    end

    return error
  end
  
  def jetty_command
    "java -Djetty.port=#{@port} -Dsolr.solr.home=#{@solr_home} -jar start.jar"
  end
  
  def start
    puts "jetty_home: #{@jetty_home}"
    puts "solr_home: #{@solr_home}"
    puts "jetty_command: #{jetty_command}"
    platform_specific_start
  end
  
  def stop
    platform_specific_stop
  end
  
  if RUBY_PLATFORM =~ /mswin32/
    require 'win32/process'

    # start the solr server
    def platform_specific_start
      Dir.chdir(@jetty_home) do
        @pid = Process.create(
              :app_name         => jetty_command,
              :creation_flags   => Process::DETACHED_PROCESS,
              :process_inherit  => false,
              :thread_inherit   => true,
              :cwd              => "#{@jetty_home}"
           ).process_id
      end
    end

    # stop a running solr server
    def platform_specific_stop
      Process.kill(1, @pid)
      Process.wait
    end
  else # Not Windows
    # start the solr server
    def platform_specific_start
      puts self.inspect
      Dir.chdir(@jetty_home) do
        @pid = fork do
          STDERR.close if @quiet
          exec jetty_command
        end
      end
    end

    # stop a running solr server
    def platform_specific_stop
      Process.kill('TERM', @pid)
      Process.wait
    end
  end

end
