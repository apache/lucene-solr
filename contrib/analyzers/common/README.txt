Lucene Contrib Analyzers README file

This project provides pre-compiled version of the Snowball stemmers
based on revision 500 of the Tartarus Snowball repository,
together with classes integrating them with the Lucene search engine.

A few changes has been made to the static Snowball code and compiled stemmers:

 * Class SnowballProgram is made abstract and contains new abstract method stem() to avoid reflection in Lucene filter class SnowballFilter.
 * All use of StringBuffers has been refactored to StringBuilder for speed.
 * Snowball BSD license header has been added to the Java classes to avoid having RAT adding new ASL headers.


IMPORTANT NOTICE ON BACKWARDS COMPATIBILITY!

An index created using the Snowball module in Lucene 2.3.2 and below
might not be compatible with the Snowball module in Lucene 2.4 or greater.

For more information about this issue see:
https://issues.apache.org/jira/browse/LUCENE-1142


For more information on Snowball, see:
  http://snowball.tartarus.org/

