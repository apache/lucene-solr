Analysis README file

INTRODUCTION

The Analysis Module provides analysis capabilities to Lucene and Solr
applications.

The Lucene web site is at:
  http://lucene.apache.org/

Please join the Lucene-User mailing list by sending a message to:
  java-user-subscribe@lucene.apache.org

FILES

lucene-analyzers-common-XX.jar
  The primary analysis module library, containing general-purpose analysis
  components and support for various languages.
  
lucene-analyzers-smartcn-XX.jar
  An add-on analysis library that provides word segmentation for Simplified
  Chinese.

lucene-analyzers-stempel-XX.jar
  An add-on analysis library that contains a universal algorithmic stemmer,
  including tables for the Polish language.

common/src/java
smartcn/src/java
stempel/src/java
  The source code for the three libraries.

common/src/test
smartcn/src/test
stempel/src/test
  Unit tests for the three libraries.
