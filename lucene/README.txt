Apache Lucene README file

INTRODUCTION

Lucene is a Java full-text search engine.  Lucene is not a complete
application, but rather a code library and API that can easily be used
to add search capabilities to applications.

The Lucene web site is at:
  http://lucene.apache.org/

Please join the Lucene-User mailing list by sending a message to:
  java-user-subscribe@lucene.apache.org

Files in a binary distribution:

lucene-core-XX.jar
  The compiled lucene library.

contrib/demo/lucene-demo-XX.jar
  The compiled simple example code.

contrib/demo/luceneweb.war
  The compiled simple example Web Application.

contrib/*
  Contributed code which extends and enhances Lucene, but is not
  part of the core library.  Of special note are the JAR files in the analyzers directory which
  contain various analyzers that people may find useful in place of the StandardAnalyzer.

docs/index.html
  The contents of the Lucene website.

docs/api/index.html
  The Javadoc Lucene API documentation.  This includes the core
  library, the demo, as well as all of the contrib modules.

See BUILD.txt for building a source distribution
