This is the README file for a search framework contribution to Lucene Sandbox.

It is an attempt at constructing a framework around the Lucene search API. 
(Can I have a name for it?)

3 interesting features of this framework are: 

datasource independence - through various datasource implementations, 
regardless of whether it is a database table, an object, a filesystem directory, 
or a website, these can all be indexed.

complex datasource support - complex datasources are containers for what are 
potentially new datasources (a Zip archive, a HTML document containing links to 
other HTML documents, a Java object which contains references to other objects 
to be indexed, etc). The framework has basic support for complex datasources.

pluggable file content handlers - content handlers which 'know' how to index 
various file formats (MS Word, Zip, Tar, etc) can be easily configured via an 
xml configuration file.