# Apache Lucene Migration Guide

## JapanesePartOfSpeechStopFilterFactory loads default stop tags if "tags" argument not specified (LUCENE-9567)

Previously, JapanesePartOfSpeechStopFilterFactory added no filter if `args` didn't include "tags". Now, it will load 
the default stop tags returned by `JapaneseAnalyzer.getDefaultStopTags()` (i.e. the tags from`stoptags.txt` in the 
`lucene-analyzers-kuromoji` jar.)

## ICUCollationKeyAnalyzer is renamed (LUCENE-9558)

o.a.l.collation.ICUCollationAnalyzer is renamed to o.a.l.a.icu.ICUCollationKeyAnalyzer.
Also, its dependant classes are renamed in the same way.

## Rename of binary artifacts from '**-analyzers-**' to '**-analysis-**' (LUCENE-9562)

All binary analysis packages (and corresponding Maven artifacts) have been renamed and are
now consistent with repository module 'analysis'. 

## Base and concrete analysis factories are moved / package renamed (LUCENE-9317)

1. Base analysis factories are moved to `lucene-core`, also their package names are renamed.

- o.a.l.a.util.TokenizerFactory (lucene-analysis-common) is moved to o.a.l.a.TokenizerFactory (lucene-core)
- o.a.l.a.util.CharFilterFactory (lucene-analysis-common) is moved to o.a.l.a.CharFilterFactory (lucene-core)
- o.a.l.a.util.TokenFilterFactory (lucene-analysis-common) is moved to o.a.l.a.TokenFilterFactory (lucene-core)

The service provider files placed in `META-INF/services` for custom analysis factories should be renamed as follows:

- META-INF/services/org.apache.lucene.analysis.TokenizerFactory
- META-INF/services/org.apache.lucene.analysis.CharFilterFactory
- META-INF/services/org.apache.lucene.analysis.TokenFilterFactory

2. o.a.l.a.standard.StandardTokenizerFactory is moved to `lucene-core` module.

3. o.a.l.a.standard package in `lucene-analysis-common` module is split into o.a.l.a.classic and o.a.l.a.email.

## RegExpQuery now rejects invalid backslashes (LUCENE-9370)

We now follow the [Java rules](https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html#bs) for accepting backslashes. 
Alphabetic characters other than s, S, w, W, d or D that are preceded by a backslash are considered illegal syntax and will throw an exception.  

## RegExp certain regular expressions now match differently (LUCENE-9336)

The commonly used regular expressions \w \W \d \D \s and \S now work the same way [Java Pattern](https://docs.oracle.com/javase/tutorial/essential/regex/pre_char_classes.html#CHART) matching works. Previously these expressions were (mis)interpreted as searches for the literal characters w, d, s etc. 

## NGramFilterFactory "keepShortTerm" option was fixed to "preserveOriginal" (LUCENE-9259)

The factory option name to output the original term was corrected in accordance with its Javadoc.

## o.a.l.misc.IndexMergeTool defaults changes (LUCENE-9206)

This command-line tool no longer forceMerges to a single segment. Instead, by
default it just follows (configurable) merge policy. If you really want to merge
to a single segment, you can pass -max-segments 1.

## o.a.l.util.fst.Builder is renamed FSTCompiler with fluent-style Builder (LUCENE-9089)

Simply use FSTCompiler instead of the previous Builder. Use either the simple constructor with default settings, or
the FSTCompiler.Builder to tune and tweak any parameter.

## Kuromoji user dictionary now forbids illegal segmentation (LUCENE-8933)

User dictionary now strictly validates if the (concatenated) segment is the same as the surface form. This change avoids
unexpected runtime exceptions or behaviours.
For example, these entries are not allowed at all and an exception is thrown when loading the dictionary file.

```
# concatenated "日本経済新聞" does not match the surface form "日経新聞"
日経新聞,日本 経済 新聞,ニホン ケイザイ シンブン,カスタム名詞

# concatenated "日経新聞" does not match the surface form "日本経済新聞"
日本経済新聞,日経 新聞,ニッケイ シンブン,カスタム名詞
```

## JapaneseTokenizer no longer emits original (compound) tokens by default when the mode is not NORMAL (LUCENE-9123)

JapaneseTokenizer and JapaneseAnalyzer no longer emits original tokens when discardCompoundToken option is not specified.
The constructor option has been introduced since Lucene 8.5.0, and the default value is changed to true.

When given the text "株式会社", JapaneseTokenizer (mode != NORMAL) emits decompounded tokens "株式" and "会社" only and no
longer outputs the original token "株式会社" by default. To output original tokens, discardCompoundToken option should be
explicitly set to false. Be aware that if this option is set to false SynonymFilter or SynonymGraphFilter does not work
correctly (see LUCENE-9173).

## Analysis factories now have customizable symbolic names (LUCENE-8778) and need additional no-arg constructor (LUCENE-9281)

The SPI names for concrete subclasses of TokenizerFactory, TokenFilterFactory, and CharfilterFactory are no longer
derived from their class name. Instead, each factory must have a static "NAME" field like this:

```
    /** o.a.l.a.standard.StandardTokenizerFactory's SPI name */
    public static final String NAME = "standard";
```

A factory can be resolved/instantiated with its NAME by using methods such as TokenizerFactory#lookupClass(String)
or TokenizerFactory#forName(String, Map<String,String>).

If there are any user-defined factory classes that don't have proper NAME field, an exception will be thrown
when (re)loading factories. e.g., when calling TokenizerFactory#reloadTokenizers(ClassLoader).

In addition starting all factories need to implement a public no-arg constructor, too. The reason for this
change comes from the fact that Lucene now uses `java.util.ServiceLoader` instead its own implementation to
load the factory classes to be compatible with Java Module System changes (e.g., load factories from modules).
In the future, extensions to Lucene developed on the Java Module System may expose the factories from their
`module-info.java` file instead of `META-INF/services`.

This constructor is never called by Lucene, so by default it throws a UnsupportedOperationException. User-defined
factory classes should implement it in the following way:

```
    /** Default ctor for compatibility with SPI */
    public StandardTokenizerFactory() {
      throw defaultCtorException();
    }
```

(`defaultCtorException()` is a protected static helper method)

## TermsEnum is now fully abstract (LUCENE-8292)

TermsEnum has been changed to be fully abstract, so non-abstract subclass must implement all it's methods.
Non-Performance critical TermsEnums can use BaseTermsEnum as a base class instead. The change was motivated
by several performance issues with FilterTermsEnum that caused significant slowdowns and massive memory consumption due
to not delegating all method from TermsEnum. See LUCENE-8292 and LUCENE-8662

## RAMDirectory, RAMFile, RAMInputStream, RAMOutputStream removed

RAM-based directory implementation have been removed. (LUCENE-8474). 
ByteBuffersDirectory can be used as a RAM-resident replacement, although it
is discouraged in favor of the default memory-mapped directory.


## Similarity.SimScorer.computeXXXFactor methods removed (LUCENE-8014)

SpanQuery and PhraseQuery now always calculate their slops as (1.0 / (1.0 +
distance)).  Payload factor calculation is performed by PayloadDecoder in the
queries module


## Scorer must produce positive scores (LUCENE-7996)

Scorers are no longer allowed to produce negative scores. If you have custom
query implementations, you should make sure their score formula may never produce
negative scores.

As a side-effect of this change, negative boosts are now rejected and
FunctionScoreQuery maps negative values to 0.


## CustomScoreQuery, BoostedQuery and BoostingQuery removed (LUCENE-8099)

Instead use FunctionScoreQuery and a DoubleValuesSource implementation.  BoostedQuery
and BoostingQuery may be replaced by calls to FunctionScoreQuery.boostByValue() and
FunctionScoreQuery.boostByQuery().  To replace more complex calculations in
CustomScoreQuery, use the lucene-expressions module:

```
SimpleBindings bindings = new SimpleBindings();
bindings.add("score", DoubleValuesSource.SCORES);
bindings.add("boost1", DoubleValuesSource.fromIntField("myboostfield"));
bindings.add("boost2", DoubleValuesSource.fromIntField("myotherboostfield"));
Expression expr = JavascriptCompiler.compile("score * (boost1 + ln(boost2))");
FunctionScoreQuery q = new FunctionScoreQuery(inputQuery, expr.getDoubleValuesSource(bindings));
```

## Index options can no longer be changed dynamically (LUCENE-8134)

Changing index options on the fly is now going to result into an
IllegalArgumentException. If a field is indexed
(FieldType.indexOptions() != IndexOptions.NONE) then all documents must have
the same index options for that field.


## IndexSearcher.createNormalizedWeight() removed (LUCENE-8242)

Instead use IndexSearcher.createWeight(), rewriting the query first, and using
a boost of 1f.

## Memory codecs removed (LUCENE-8267)

Memory codecs have been removed from the codebase (MemoryPostings, MemoryDocValues).

## Direct doc-value format removed (LUCENE-8917)

The "Direct" doc-value format has been removed from the codebase.

## QueryCachingPolicy.ALWAYS_CACHE removed (LUCENE-8144)

Caching everything is discouraged as it disables the ability to skip non-interesting documents.
ALWAYS_CACHE can be replaced by a UsageTrackingQueryCachingPolicy with an appropriate config.

## English stopwords are no longer removed by default in StandardAnalyzer (LUCENE_7444)

To retain the old behaviour, pass EnglishAnalyzer.ENGLISH_STOP_WORDS_SET as an argument
to the constructor

## StandardAnalyzer.ENGLISH_STOP_WORDS_SET has been moved

English stop words are now defined in EnglishAnalyzer#ENGLISH_STOP_WORDS_SET in the
analysis-common module

## TopDocs.maxScore removed

TopDocs.maxScore is removed. IndexSearcher and TopFieldCollector no longer have
an option to compute the maximum score when sorting by field. If you need to
know the maximum score for a query, the recommended approach is to run a
separate query:

```
  TopDocs topHits = searcher.search(query, 1);
  float maxScore = topHits.scoreDocs.length == 0 ? Float.NaN : topHits.scoreDocs[0].score;
```

Thanks to other optimizations that were added to Lucene 8, this query will be
able to efficiently select the top-scoring document without having to visit
all matches.

## TopFieldCollector always assumes fillFields=true

Because filling sort values doesn't have a significant overhead, the fillFields
option has been removed from TopFieldCollector factory methods. Everything
behaves as if it was set to true.

## TopFieldCollector no longer takes a trackDocScores option

Computing scores at collection time is less efficient than running a second
request in order to only compute scores for documents that made it to the top
hits. As a consequence, the trackDocScores option has been removed and can be
replaced with the new TopFieldCollector#populateScores helper method.

## IndexSearcher.search(After) may return lower bounds of the hit count and TopDocs.totalHits is no longer a long

Lucene 8 received optimizations for collection of top-k matches by not visiting
all matches. However these optimizations won't help if all matches still need
to be visited in order to compute the total number of hits. As a consequence,
IndexSearcher's search and searchAfter methods were changed to only count hits
accurately up to 1,000, and Topdocs.totalHits was changed from a long to an
object that says whether the hit count is accurate or a lower bound of the
actual hit count.

## RAMDirectory, RAMFile, RAMInputStream, RAMOutputStream are deprecated

This RAM-based directory implementation is an old piece of code that uses inefficient
thread synchronization primitives and can be confused as "faster" than the NIO-based
MMapDirectory. It is deprecated and scheduled for removal in future versions of 
Lucene. (LUCENE-8467, LUCENE-8438)

## LeafCollector.setScorer() now takes a Scorable rather than a Scorer

Scorer has a number of methods that should never be called from Collectors, for example
those that advance the underlying iterators.  To hide these, LeafCollector.setScorer()
now takes a Scorable, an abstract class that Scorers can extend, with methods
docId() and score() (LUCENE-6228)

## Scorers must have non-null Weights

If a custom Scorer implementation does not have an associated Weight, it can probably
be replaced with a Scorable instead.

## Suggesters now return Long instead of long for weight() during indexing, and double instead of long at suggest time 

Most code should just require recompilation, though possibly requiring some added casts.

## TokenStreamComponents is now final

Instead of overriding TokenStreamComponents#setReader() to customise analyzer
initialisation, you should now pass a Consumer&lt;Reader> instance to the
TokenStreamComponents constructor.

## LowerCaseTokenizer and LowerCaseTokenizerFactory have been removed

LowerCaseTokenizer combined tokenization and filtering in a way that broke token
normalization, so they have been removed. Instead, use a LetterTokenizer followed by
a LowerCaseFilter

## CharTokenizer no longer takes a normalizer function ##

CharTokenizer now only performs tokenization. To perform any type of filtering
use a TokenFilter chain as you would with any other Tokenizer.

## Highlighter and FastVectorHighlighter no longer support ToParent/ToChildBlockJoinQuery

Both Highlighter and FastVectorHighlighter need a custom WeightedSpanTermExtractor or FieldQuery respectively
in order to support ToParent/ToChildBlockJoinQuery.

## MultiTermAwareComponent replaced by CharFilterFactory#normalize() and TokenFilterFactory#normalize()

Normalization is now type-safe, with CharFilterFactory#normalize() returning a Reader and
TokenFilterFactory#normalize() returning a TokenFilter.

## k1+1 constant factor removed from BM25 similarity numerator (LUCENE-8563)

Scores computed by the BM25 similarity are lower than previously as the k1+1
constant factor was removed from the numerator of the scoring formula.
Ordering of results is preserved unless scores are computed from multiple
fields using different similarities. The previous behaviour is now exposed
by the LegacyBM25Similarity class which can be found in the lucene-misc jar.

## IndexWriter#maxDoc()/#numDocs() removed in favor of IndexWriter#getDocStats()

IndexWriter#getDocStats() should be used instead of #maxDoc() / #numDocs() which offers a consistent 
view on document stats. Previously calling two methods in order ot get point in time stats was subject
to concurrent changes.

## maxClausesCount moved from BooleanQuery To IndexSearcher (LUCENE-8811)

IndexSearcher now performs max clause count checks on all types of queries (including BooleanQueries).
This led to a logical move of the clauses count from BooleanQuery to IndexSearcher.

## TopDocs.merge shall no longer allow setting of shard indices

TopDocs.merge's API has been changed to stop allowing passing in a parameter to indicate if it should
set shard indices for hits as they are seen during the merge process. This is done to simplify the API
to be more dynamic in terms of passing in custom tie breakers.
If shard indices are to be used for tie breaking docs with equal scores during TopDocs.merge, then it is
mandatory that the input ScoreDocs have their shard indices set to valid values prior to calling TopDocs.merge

## TopDocsCollector Shall Throw IllegalArgumentException For Malformed Arguments

TopDocsCollector shall no longer return an empty TopDocs for malformed arguments.
Rather, an IllegalArgumentException shall be thrown. This is introduced for better
defence and to ensure that there is no bubbling up of errors when Lucene is
used in multi level applications

## Assumption of data consistency between different data-structures sharing the same field name

Sorting on a numeric field that is indexed with both doc values and points may use an
optimization to skip non-competitive documents. This optimization relies on the assumption
that the same data is stored in these points and doc values.
