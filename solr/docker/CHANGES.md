This file lists release notes for this module.
Prior to version 9, the module existed in another repository and changes were not tracked in this manner.
You could browse the commit history here instead:
https://github.com/docker-solr/docker-solr
 
9.0.0
==================

Improvements
----------------------
* SOLR-14001: Removed /var/solr initialization from the Dockerfile; depend on init_var_solr.sh instead.
  This leads to more consistent behavior no matter how /var/solr is mounted.
  * init_var_solr.sh is now invoked by docker-entrypoint.sh; not in a bunch of other places.
  * as before, you can set NO_INIT_VAR_SOLR=1 to short-circuit this.
  * init_var_solr.sh no longer echo's anything.  For verbosity, set VERBOSE=yes.
  (David Smiley)

* SOLR-14957: Add Prometheus Exporter to docker PATH. Fix classpath issues.
  (Houston Putman)
  
* SOLR-14949: Ability to customize the FROM image when building.
  (Houston Putman)

Other Changes
------------------
* SOLR-14789: Migrate docker image creation from docker-solr repo to solr/docker. 
  (Houston Putman, Martijn Koster, Tim Potter, David Smiley, janhoy, Mike Drob)

