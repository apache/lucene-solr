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

lucene-analyzers-icu-XX.jar
  An add-on analysis library that provides improved Unicode support via
  International Components for Unicode (ICU). Note: this module depends on
  the ICU4j jar file (version >= 4.6.0)

lucene-analyzers-phonetic-XX.jar
  An add-on analysis library that provides phonetic encoders via Apache
  Commons-Codec. Note: this module depends on the commons-codec jar 
  file (version >= 1.4)
  
lucene-analyzers-smartcn-XX.jar
  An add-on analysis library that provides word segmentation for Simplified
  Chinese.

lucene-analyzers-stempel-XX.jar
  An add-on analysis library that contains a universal algorithmic stemmer,
  including tables for the Polish language.

lucene-analyzers-morfologik-XX.jar
  An analyzer using the Morfologik stemming library.

common/src/java
icu/src/java
phonetic/src/java
smartcn/src/java
stempel/src/java
morfologik/src/java
  The source code for the ffve libraries.

common/src/test
icu/src/test
phonetic/src/test
smartcn/src/test
stempel/src/test
morfologik/src/test
  Unit tests for the five libraries.
