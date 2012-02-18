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
  The compiled Lucene library.

lucene-core-XX-javadoc.jar
  The Javadoc jar for the compiled Lucene library.
  
lucene-test-framework-XX.jar
  The compiled Lucene test-framework library.
  Depends on junit 4.10.x (not 4.9.x, not 4.11.x), and Apache Ant 1.7.x (not 1.6.x, not 1.8.x)

lucene-test-framework-XX-javadoc.jar
  The Javadoc jar for the compiled Lucene test-framework library.

contrib/demo/lucene-demo-XX.jar
  The compiled simple example code.

contrib/*
  Contributed code which extends and enhances Lucene, but is not
  part of the core library.  Of special note are the JAR files in the analyzers directory which
  contain various analyzers that people may find useful in place of the StandardAnalyzer.

docs/index.html
  The contents of the Lucene website.

docs/api/index.html
  The Javadoc Lucene API documentation.  This includes the core library, 
  the test framework, and the demo, as well as all of the contrib modules.
