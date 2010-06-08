In trunk this contrib module is currently disabled, as it uses the external
(binary) Carrot2 library (as trunk is free to change its API, this module fails
with linking exceptions).

After a stable branch of Lucene is created from trunk, rename
'build.xml.disabled' back to 'build.xml' after replacing the Carrot2
JARs by updated versions.
