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


# the default task is to run both the unit and functional tests
# functional tests require that a solr test server is running
# but this Rakefil should take care of starting and stopping it 
# for you
# 
# if you just want to run unit tests:
#
#    rake test_units
#
# and if you just want to run functional tests
#
#    rake test_functionals
#
# if you would like to see solr startup messages on STDERR
# when starting solr test server during functional tests use:
# 
#    rake SOLR_CONSOLE=true

SOLR_RUBY_VERSION = '0.0.8'

require 'rubygems'
require 'rake'
require 'rake/testtask'
require 'rake/rdoctask'
require 'rake/packagetask'
require 'rake/gempackagetask'
require 'test/functional/test_solr_server'

task :default => [:test_units]

SOLR_PARAMS = {
  :quiet => ENV['SOLR_CONSOLE'] ? false : true,
  :jetty_home => ENV['SOLR_JETTY_HOME'] || File.expand_path('../../../example'),
  :jetty_port => ENV['SOLR_JETTY_PORT'] || 8888,
  :solr_home => ENV['SOLR_HOME'] || File.expand_path('test')
}


spec = Gem::Specification.new do |s|
  s.name = 'solr-ruby'
  s.version = SOLR_RUBY_VERSION
  s.author = 'Apache Solr'
  s.email = 'ruby-dev@lucene.apache.org'
  s.homepage = 'http://wiki.apache.org/solr/solr-ruby'
  s.platform = Gem::Platform::RUBY
  s.summary = 'Ruby library for working with Apache Solr'
  
  # Omit functional tests from gem for now, as that requires a Solr instance
  s.files = Dir.glob("lib/**/*").concat(Dir.glob("test/unit/**/*"))
  s.require_path = 'lib'
  s.autorequire = 'solr'
  s.has_rdoc = true
end

namespace :gem do
  Rake::GemPackageTask.new(spec) do |pkg|
    pkg.need_zip = true
    pkg.need_tar = true
    pkg.package_dir = "pkg/gem"
  end
end

namespace :rails do
  desc "Creates rails plugin structure and distributable packages. init.rb is created and removed on the fly."
  task :package => "init.rb" do
    File.rm_f("init.rb")
  end
  Rake::PackageTask.new("solr-ruby-rails", SOLR_RUBY_VERSION) do |pkg|
    pkg.need_zip = true
    pkg.need_tar = true
    pkg.package_dir = "pkg/rails"
    pkg.package_files.include("lib/**/*.rb", "test/unit/**/*.rb", "init.rb", "LICENSE.txt", "README")
  end
  
  file "init.rb" do
    open("init.rb", "w") do |file|
      file.puts LICENSE
      file.puts "require 'solr.rb'"
    end
  end
  
  desc "Install the Rails plugin version into the vendor/plugins dir. Need to set PLUGINS_DIR environment variable."
  task :install_solr_ruby => :package do
    plugins_dir = ENV["PLUGINS_DIR"] or raise "You must set PLUGINS_DIR"
    mkdir File.join(plugins_dir, "solr-ruby-rails-#{SOLR_RUBY_VERSION}/") rescue nil
    File.cp_r(File.join("pkg","rails", "solr-ruby-rails-#{SOLR_RUBY_VERSION}/"), plugins_dir)
  end
end

task :package => ["rails:package", "gem:package"]
task :repackage => [:clobber_package, :package]
task :clobber_package => ["rails:clobber_package", "gem:clobber_package"] do rm_r "pkg" rescue nil end
task :clobber => [:clobber_package]

desc "Generate rdoc documentation"
Rake::RDocTask.new('doc') do |rd|
  rd.rdoc_files.include("lib/**/*.rb")
  rd.rdoc_files.include('README', 'CHANGES.yml', 'LICENSE.txt')
  rd.main = 'README'
  rd.rdoc_dir = 'doc'
end

desc "Run unit tests"
Rake::TestTask.new(:test_units) do |t|
  t.pattern = 'test/unit/*_test.rb'
  t.verbose = true
  t.ruby_opts = ['-r solr', '-r test/unit', '-Itest/unit']
end

# NOTE: test_functionals does not work standalone currently.  It needs the TestSolrServer wrapper in the :test task
Rake::TestTask.new(:test_functionals) do |t|
  t.pattern = 'test/functional/*_test.rb'
  t.verbose = true
  t.ruby_opts = ['-r solr', '-r test/unit', '-Itest/functional']
end

desc "Run unit and functional tests"
task :test => [:test_units] do
  rm_rf "test/data"  # remove functional test temp data directory
  
  # wrap functional tests with a test-specific Solr server
  got_error = TestSolrServer.wrap(SOLR_PARAMS) do
    Rake::Task[:test_functionals].invoke 
  end

  raise "test failures" if got_error
end

# TODO: consider replacing system() to rcov with the included
#       Rake task: http://eigenclass.org/hiki.rb?cmd=view&p=rcov+FAQ&key=rake
namespace :test do
  desc 'Measures test coverage'
  # borrowed from here: http://clarkware.com/cgi/blosxom/2007/01/05#RcovRakeTask
  task :coverage do
    rm_rf "coverage"
    rm_rf "coverage.data"
    TestSolrServer.wrap(SOLR_PARAMS) do
      system("rcov --aggregate coverage.data --text-summary -Ilib:test/functional test/functional/*_test.rb")
    end
    system("rcov --aggregate coverage.data --text-summary -Ilib:test/unit test/unit/*_test.rb")
    system("open coverage/index.html") if PLATFORM['darwin']
  end
end


def egrep(pattern)
  Dir['**/*.rb'].each do |fn|
    count = 0
    open(fn) do |f|
      while line = f.gets
        count += 1
        if line =~ pattern
          puts "#{fn}:#{count}:#{line}"
        end
      end
    end
  end
end

desc "Report TODO/FIXME/TBD tags in the code"
task :todo do
  egrep /#.*(FIXME|TODO|TBD)/
end

LICENSE = <<STR
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
STR
