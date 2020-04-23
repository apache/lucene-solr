# JRE Version Migration Guide

If possible, use the same JRE major version at both index and search time.
When upgrading to a different JRE major version, consider re-indexing. 

Different JRE major versions may implement different versions of Unicode,
which will change the way some parts of Lucene treat your text.

For example: with Java 1.4, `LetterTokenizer` will split around the character U+02C6,
but with Java 5 it will not.
This is because Java 1.4 implements Unicode 3, but Java 5 implements Unicode 4.

For reference, JRE major versions with their corresponding Unicode versions:

 * Java 1.4, Unicode 3.0
 * Java 5, Unicode 4.0
 * Java 6, Unicode 4.0
 * Java 7, Unicode 6.0
 * Java 8, Unicode 6.2
 * Java 9, Unicode 8.0

In general, whether you need to re-index largely depends upon the data that
you are searching, and what was changed in any given Unicode version. For example, 
if you are completely sure your content is limited to the "Basic Latin" range
of Unicode, you can safely ignore this. 

## Special Notes: LUCENE 2.9 TO 3.0, JAVA 1.4 TO JAVA 5 TRANSITION

* `StandardAnalyzer` will return the same results under Java 5 as it did under 
Java 1.4. This is because it is largely independent of the runtime JRE for
Unicode support, (except for lowercasing).  However, no changes to
casing have occurred in Unicode 4.0 that affect StandardAnalyzer, so if you are 
using this Analyzer you are NOT affected.

* `SimpleAnalyzer`, `StopAnalyzer`, `LetterTokenizer`, `LowerCaseFilter`, and 
`LowerCaseTokenizer` may return different results, along with many other `Analyzer`s
and `TokenStream`s in Lucene's analysis modules. If you are using one of these 
components, you may be affected.

