Lucene Analyzers README file

This project provides pre-compiled version of the Snowball stemmers,
now located at https://github.com/snowballstem/snowball/tree/53739a805cfa6c77ff8496dc711dc1c106d987c1 (GitHub),
together with classes integrating them with the Lucene search engine.

The snowball tree needs patches applied to properly generate efficient code for lucene.
You can find the most recent patch on https://issues.apache.org/jira/browse/LUCENE-9220

IMPORTANT NOTICE ON BACKWARDS COMPATIBILITY!

An index created using the Snowball module in Lucene 2.3.2 and below
might not be compatible with the Snowball module in Lucene 2.4 or greater.

For more information about this issue see:
https://issues.apache.org/jira/browse/LUCENE-1142


For more information on Snowball, see:
  http://snowball.tartarus.org/

