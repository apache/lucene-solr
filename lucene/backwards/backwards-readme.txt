* This is a placeholder for 4.1, when 4.0 will be branched *

============== DISABLED =============================================================================

This folder contains the src/ folder of the previous Lucene major version.

The test-backwards ANT task compiles the core classes of the previous version and its tests
against these class files. After that the compiled test classes are run against the new
lucene-core.jar file.

After branching a new Lucene major version (branch name "lucene_X_Y") do the following:

* svn rm backwards/src/
* svn cp https://svn.apache.org/repos/asf/lucene/dev/branches/lucene_X_Y/lucene/src/ backwards/src/
* Check that everything is correct: The backwards folder should contain a src/ folder
  that now contains java, test, demo,.... The files should be the ones from the branch.
* Run "ant test-backwards"
