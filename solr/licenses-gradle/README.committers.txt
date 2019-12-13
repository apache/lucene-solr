
         --------------------------
         Information For Committers
         --------------------------

Under no circumstances should any new files be added to this directory
without careful consideration of how LICENSE.txt and NOTICE.txt in the
parent directory should be updated to reflect the addition. 

Even if a Jar being added is from another Apache project, it should be
mentioned in NOTICE.txt, and may have additional Attribution or
Licencing information that also needs to be added to the appropriate
file.  

---

If an existing Jar is replaced with a newer version, the same
consideration should be given as if it were an entirely new file:
verify that no updates need to be made to LICENSE.txt or NOTICE.txt
based on changes in the terms of the dependency being updated. 

---

When adding a jar or updating an existing jar, be sure to include/update 
xyz-LICENSE.txt and if applicable, xyz-NOTICE.txt.  These files often
change across versions of the dependency, so when updating be SURE to 
update them to the recent version. This also allows others to see
what changed with respect to licensing in the commit diff.

---

Any changes made to this directory should be noted in CHANGES.txt,
along with the specific version information.  If the version is a
"snapshot" of another Apache project, include the SVN revision number.

---

When upgrading Lucene-Java Jars, remember to generate new Analysis
factories for any new Tokenizers or TokenFilters.  See the wiki for
details...

  http://wiki.apache.org/solr/CommitterInfo
