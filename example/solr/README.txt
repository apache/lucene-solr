Example "Solr Home" Directory
=============================

This directory is provided as an example of what a "Solr Home" directory
should look like.

It's not strictly necessary that you copy all of the files in this
directory when setting up a new instance of Solr, but it is recommended.


Basic Directory Structure
-------------------------

The Solr Home directory typically contains the following subdirectories...

   conf/
        This directory is mandatory and must contain your solrconfig.xml
        and schema.xml.  Any other optional configuration files would also 
        be kept here.

   data/
        This directory is the default location where Solr will keep your
        index, and is used by the replication scripts for dealing with
        snapshots.  You can override this location in the solrconfig.xml
        and scripts.conf files. Solr will create this directory if it
        does not already exist.

   lib/
        This directory is optional.  If it exists, Solr will load any Jars
        found in this directory and use them to resolve any "plugins"
        specified in your solrconfig.xml or schema.xml (ie: Analyzers,
        Request Handlers, etc...)

   bin/
        This directory is optional.  It is the default location used for
        keeping the replication scripts.
