This folder contains the src/ folder of the previous Lucene major version.

The test-backwards ANT task compiles the previous version's tests (bundled) against the
previous released lucene-core.jar file (bundled). After that the compiled test classes
are run against the new lucene-core.jar file, created by ANT before.

After branching a new Lucene major version (branch name "lucene_X_Y") do the following:

* svn rm backwards/src/test
* svn cp https://svn.apache.org/repos/asf/lucene/dev/branches/lucene_X_Y/lucene/src/test-framework backwards/src
* svn cp https://svn.apache.org/repos/asf/lucene/dev/branches/lucene_X_Y/lucene/src/test backwards/src
* Copy the lucene-core.jar from the last release tarball to backwards/lib and delete old one.
* Check that everything is correct: The backwards folder should contain a src/ folder
  that now contains "test" and "test-framework". The files should be the ones from the branch.
* Run "ant test-backwards"
