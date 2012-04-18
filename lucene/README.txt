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

Files are organized by module, for example in core/:

core/lucene-core-XX.jar
  The compiled core Lucene library.

Additional modules contain the same structure:

analysis/common/: Analyzers for indexing content in different languages and domains
analysis/kuromoji/: Analyzer for indexing Japanese
analysis/morfologik/: Analyzer for indexing Polish
analysis/phonetic/: Analyzer for indexing phonetic signatures (for sounds-alike search)
analysis/smartcn/: Analyzer for indexing Chinese
analysis/stempel/: Analyzer for indexing Polish
analysis/uima/: Analyzer that integrates with Apache UIMA
benchmark/: System for benchmarking Lucene
demo/: Simple example code
facet/: Faceted indexing and search capabilities
grouping/: Search result grouping
highlighter/: Highlights search keywords in results
join/: Index-time and Query-time joins for normalized content
memory/: Single-document in memory index implementation
misc/: Index tools and other miscellaneous code
queries/: Filters and Queries that add to core Lucene
queryparser/: Query parsers and parsing framework
sandbox/: Various third party contributions and new ideas.
spatial/: Geospatial search
suggest/: Auto-suggest and Spellchecking support
test-framework/:  Test Framework for testing Lucene-based applications
  
docs/index.html
  The contents of the Lucene website.

docs/api/index.html
  The Javadoc Lucene API documentation.  This includes the core library, 
  the test framework, and the demo, as well as all other modules.
