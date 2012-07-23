The analysis-extras plugin provides additional analyzers that rely
upon large dependencies/dictionaries.

It includes integration with ICU for multilingual support, and 
analyzers for Chinese and Polish.

Relies upon the following lucene components (in lucene-libs/):

 * lucene-analyzers-icu-X.Y.jar
 * lucene-analyzers-smartcn-X.Y.jar
 * lucene-analyzers-stempel-X.Y.jar
 * lucene-analyzers-morfologik-X.Y.jar
 * lucene-analyzers-smartcn-X.Y.jar

And the following third-party library (in lib/):

 * icu4j-X.Y.jar
 * morfologik-*.jar
 
