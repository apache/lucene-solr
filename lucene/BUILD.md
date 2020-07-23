# Lucene Build Instructions

## Basic steps:
  
  0. Install OpenJDK 11 (or greater)
  1. Download Lucene/Solr from Apache and unpack it
  2. Connect to the top-level of your installation (parent of the lucene top-level directory)
  3. Install JavaCC (optional)
  4. Run gradle

## Step 0) Set up your development environment (OpenJDK 11 or greater)

We'll assume that you know how to get and set up the JDK - if you
don't, then we suggest starting at https://www.oracle.com/java/ and learning
more about Java, before returning to this README. Lucene runs with
Java 11 and later.

Lucene uses [Gradle](https://gradle.org/) for build control; and includes Gradle wrapper script to download the correct version of it.

NOTE: When Solr moves to a Top Level Project, it will no longer
be necessary to download Solr to build Lucene. You can track
progress at: https://issues.apache.org/jira/browse/SOLR-14497 

NOTE: Lucene changed from Ant to Gradle as of release 9.0. Prior releases
still use Ant.

## Step 1) Download/Checkout Lucene source code

We'll assume you already did this, or you wouldn't be reading this
file.  However, you might have received this file by some alternate
route, or you might have an incomplete copy of the Lucene, so: Lucene
releases are available as part of Solr for download at:

  https://lucene.apache.org/solr/downloads.html
  
See the note above for why it is necessary currently to download Solr

Download either a zip or a tarred/gzipped version of the archive, and
uncompress it into a directory of your choice.

Or you can directly checkout the source code from GitHub:

  https://github.com/apache/lucene-solr

## Step 2) From the command line, change (cd) into the top-level directory of your Lucene/Solr installation

The parent directory for both Lucene and Solr contains the base configuration
file for the combined build, as well as the "gradle wrapper" (gradlew) that
makes invocation of Gradle easier. By default, you do not need to change any of 
the settings in this file, but you do need to run Gradle from this location so 
it knows where to find the necessary configurations.

The first time you run Gradle, it will create a file "gradle.properties" that
contains machine-specific settings. Normally you can use this file as-is, but it
can be modified if necessary. 

## Step 4) Run Gradle

Assuming you can exectue "./gradlew help" should show you the main tasks that
can be executed to show help sub-topics.

If you want to build Lucene independent of Solr, type:
  ./gradlew -p lucene assemble

If you want to build the documentation, type "./gradlew buildSite".

For further information on Lucene, go to:

  https://lucene.apache.org/

Please join the Lucene-User mailing list by visiting this site:

  https://lucene.apache.org/core/discussion.html

Please post suggestions, questions, corrections or additions to this
document to the lucene-user mailing list.

