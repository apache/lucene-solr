Lucene Analyzers README file

This project provides pre-compiled version of the Snowball stemmers
based on revision 502 of the Tartarus Snowball repository,
now located at https://github.com/snowballstem/snowball/tree/e103b5c257383ee94a96e7fc58cab3c567bf079b (GitHub),
together with classes integrating them with the Lucene search engine.

A few changes has been made to the static Snowball code and compiled stemmers:

 * Class SnowballProgram is made abstract and contains new abstract method stem() to avoid reflection in Lucene filter class SnowballFilter.
 * All use of StringBuffers has been refactored to StringBuilder for speed.
 * Snowball BSD license header has been added to the Java classes to avoid having RAT adding new ASL headers.
 * Uses Java 7 MethodHandles and fixes method visibility bug: http://article.gmane.org/gmane.comp.search.snowball/1139

If you want to add new stemmers, use the exact revision / Git commit above to generate the Java class, place it
in src/java/org/tartarus/snowball/ext, and finally execute "ant patch-snowball". The latter will change the APIs
of the generated class to make it compatible. Already patched classes are not modified.
The Arabic stemmer has been generated from https://github.com/snowballstem/snowball/blob/master/algorithms/arabic.sbl
using the latest version of snowball and patched manually.

IMPORTANT NOTICE ON BACKWARDS COMPATIBILITY!

An index created using the Snowball module in Lucene 2.3.2 and below
might not be compatible with the Snowball module in Lucene 2.4 or greater.

For more information about this issue see:
https://issues.apache.org/jira/browse/LUCENE-1142


For more information on Snowball, see:
  http://snowball.tartarus.org/

