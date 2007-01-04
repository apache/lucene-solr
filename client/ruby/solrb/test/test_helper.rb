#!/usr/bin/ruby

def start_solr_server
  puts "__FILE__ = #{File.dirname(__FILE__)}"
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
