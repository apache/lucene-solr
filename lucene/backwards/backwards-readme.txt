This folder contains the src/ folder of the previous Lucene major version.

The test-backwards ANT task compiles the previous version's tests (bundled) against the
previous released lucene-core.jar file (bundled). After that the compiled test classes
are run against the new lucene-core.jar file, created by ANT before.

After tagging a new Lucene *major* version (tag name "lucene_solr_X_Y_0") do the following
(for minor versions never do this); also always use the x.y.0 version for the backwards folder,
later bugfix releases should not be tested (the reason is that the new version must be backwards
compatible to the last base version, bugfixes should not taken into account):

* cd lucene/backwards
* svn rm src/test src/test-framework lib/lucene-core*.jar
* svn commit (1st commit; you must do this, else you will corrupt your checkout)
* svn cp https://svn.apache.org/repos/asf/lucene/dev/tags/lucene_solr_X_Y_0/lucene/src/test-framework src
* svn cp https://svn.apache.org/repos/asf/lucene/dev/tags/lucene_solr_X_Y_0/lucene/src/test src
* Copy the lucene-core.jar from the last release tarball to lib.
* Check that everything is correct: The backwards folder should contain a src/ folder
  that now contains "test" and "test-framework". The files should be the ones from the last version.
* Run "ant test-backwards"
* Commit the stuff again (2nd commit)
