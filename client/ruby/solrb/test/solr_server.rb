#!/usr/bin/ruby

# TODO: would be nice to be able to start up
# solr on another port than the default one
# so we don't interfere with one that's currently
# running.

def start_solr
  Dir.chdir(File.dirname(__FILE__) + '/../solr') do 
    puts "starting solr server"

    # subprocess to run our solr server 
    $SOLR_PID = fork do

      # dissassociate from the process group for the unit tests
      Process.setsid

      # don't want to see the messages about solr starting up
      STDERR.close 

      # start 'er up
      system 'java -Djetty.port=8888 -jar start.jar'
    end
  end

  # wait for the jvm and solr to start
  sleep 5 
end

def stop_solr
  puts "stopping solr server"
  # stop all the processes we can by terminating the group
  process_group_id = Process.getpgid($SOLR_PID)
  Process.kill(-15, process_group_id)
end

if __FILE__ == $0
  start_solr
  stop_solr
end

