# Apache Lucene Migration Guide

## Four-dimensional enumerations

Flexible indexing changed the low level fields/terms/docs/positions
enumeration APIs.  Here are the major changes:

  * Terms are now binary in nature (arbitrary byte[]), represented
    by the BytesRef class (which provides an offset + length "slice"
    into an existing byte[]).

  * Fields are separately enumerated (Fields.iterator()) from the terms
    within each field (TermEnum).  So instead of this:

        TermEnum termsEnum = ...;
        while(termsEnum.next()) {
          Term t = termsEnum.term();
          System.out.println("field=" + t.field() + "; text=" + t.text());
        }

    Do this:

        for(String field : fields) {
            Terms terms = fields.terms(field);
            TermsEnum termsEnum = terms.iterator(null);
            BytesRef text;
            while((text = termsEnum.next()) != null) {
              System.out.println("field=" + field + "; text=" + text.utf8ToString());
          }
        }

  * TermDocs is renamed to DocsEnum.  Instead of this:

        while(td.next()) {
          int doc = td.doc();
          ...
        }

    do this:

        int doc;
        while((doc = td.next()) != DocsEnum.NO_MORE_DOCS) {
          ...
        }

    Instead of this:
    
        if (td.skipTo(target)) {
          int doc = td.doc();
          ...
        }

    do this:
    
        if ((doc=td.advance(target)) != DocsEnum.NO_MORE_DOCS) {
          ...
        }

  * TermPositions is renamed to DocsAndPositionsEnum, and no longer
    extends the docs only enumerator (DocsEnum).

  * Deleted docs are no longer implicitly filtered from
    docs/positions enums.  Instead, you pass a Bits
    skipDocs (set bits are skipped) when obtaining the enums.  Also,
    you can now ask a reader for its deleted docs.

  * The docs/positions enums cannot seek to a term.  Instead,
    TermsEnum is able to seek, and then you request the
    docs/positions enum from that TermsEnum.

  * TermsEnum's seek method returns more information.  So instead of
    this:

        Term t;
        TermEnum termEnum = reader.terms(t);
        if (t.equals(termEnum.term())) {
          ...
        }

    do this:

        TermsEnum termsEnum = ...;
        BytesRef text;
        if (termsEnum.seek(text) == TermsEnum.SeekStatus.FOUND) {
          ...
        }

    SeekStatus also contains END (enumerator is done) and NOT_FOUND
    (term was not found but enumerator is now positioned to the next
    term).

  * TermsEnum has an ord() method, returning the long numeric
    ordinal (ie, first term is 0, next is 1, and so on) for the term
    it's not positioned to.  There is also a corresponding seek(long
    ord) method.  Note that these methods are optional; in
    particular the MultiFields TermsEnum does not implement them.


  * How you obtain the enums has changed.  The primary entry point is
    the Fields class.  If you know your reader is a single segment
    reader, do this:

        Fields fields = reader.Fields();
        if (fields != null) {
          ...
        }

    If the reader might be multi-segment, you must do this:
    
        Fields fields = MultiFields.getFields(reader);
        if (fields != null) {
          ...
        }
  
    The fields may be null (eg if the reader has no fields).

    Note that the MultiFields approach entails a performance hit on
    MultiReaders, as it must merge terms/docs/positions on the fly. It's
    generally better to instead get the sequential readers (use
    oal.util.ReaderUtil) and then step through those readers yourself,
    if you can (this is how Lucene drives searches).

    If you pass a SegmentReader to MultiFields.fields it will simply
    return reader.fields(), so there is no performance hit in that
    case.

    Once you have a non-null Fields you can do this:

        Terms terms = fields.terms("field");
        if (terms != null) {
          ...
        }

    The terms may be null (eg if the field does not exist).

    Once you have a non-null terms you can get an enum like this:

        TermsEnum termsEnum = terms.iterator();

    The returned TermsEnum will not be null.

    You can then .next() through the TermsEnum, or seek.  If you want a
    DocsEnum, do this:

        Bits liveDocs = reader.getLiveDocs();
        DocsEnum docsEnum = null;

        docsEnum = termsEnum.docs(liveDocs, docsEnum, needsFreqs);

    You can pass in a prior DocsEnum and it will be reused if possible.

    Likewise for DocsAndPositionsEnum.

    IndexReader has several sugar methods (which just go through the
    above steps, under the hood).  Instead of:

        Term t;
        TermDocs termDocs = reader.termDocs();
        termDocs.seek(t);

    do this:

        Term t;
        DocsEnum docsEnum = reader.termDocsEnum(t);

    Likewise for DocsAndPositionsEnum.

## LUCENE-2380: FieldCache.getStrings/Index --> FieldCache.getDocTerms/Index

  * The field values returned when sorting by SortField.STRING are now
    BytesRef.  You can call value.utf8ToString() to convert back to
    string, if necessary.

  * In FieldCache, getStrings (returning String[]) has been replaced
    with getTerms (returning a FieldCache.DocTerms instance).
    DocTerms provides a getTerm method, taking a docID and a BytesRef
    to fill (which must not be null), and it fills it in with the
    reference to the bytes for that term.

    If you had code like this before:

        String[] values = FieldCache.DEFAULT.getStrings(reader, field);
        ...
        String aValue = values[docID];

    you can do this instead:

        DocTerms values = FieldCache.DEFAULT.getTerms(reader, field);
        ...
        BytesRef term = new BytesRef();
        String aValue = values.getTerm(docID, term).utf8ToString();

    Note however that it can be costly to convert to String, so it's
    better to work directly with the BytesRef.

  * Similarly, in FieldCache, getStringIndex (returning a StringIndex
    instance, with direct arrays int[] order and String[] lookup) has
    been replaced with getTermsIndex (returning a
    FieldCache.DocTermsIndex instance).  DocTermsIndex provides the
    getOrd(int docID) method to lookup the int order for a document,
    lookup(int ord, BytesRef reuse) to lookup the term from a given
    order, and the sugar method getTerm(int docID, BytesRef reuse)
    which internally calls getOrd and then lookup.

    If you had code like this before:

        StringIndex idx = FieldCache.DEFAULT.getStringIndex(reader, field);
        ...
        int ord = idx.order[docID];
        String aValue = idx.lookup[ord];

    you can do this instead:

        DocTermsIndex idx = FieldCache.DEFAULT.getTermsIndex(reader, field);
        ...
        int ord = idx.getOrd(docID);
        BytesRef term = new BytesRef();
        String aValue = idx.lookup(ord, term).utf8ToString();

    Note however that it can be costly to convert to String, so it's
    better to work directly with the BytesRef.

    DocTermsIndex also has a getTermsEnum() method, which returns an
    iterator (TermsEnum) over the term values in the index (ie,
    iterates ord = 0..numOrd()-1).

  * StringComparatorLocale is now more CPU costly than it was before
    (it was already very CPU costly since it does not compare using
    indexed collation keys; use CollationKeyFilter for better
    performance), since it converts BytesRef -> String on the fly.
    Also, the field values returned when sorting by SortField.STRING
    are now BytesRef.

  * FieldComparator.StringOrdValComparator has been renamed to
    TermOrdValComparator, and now uses BytesRef for its values.
    Likewise for StringValComparator, renamed to TermValComparator.
    This means when sorting by SortField.STRING or
    SortField.STRING_VAL (or directly invoking these comparators) the
    values returned in the FieldDoc.fields array will be BytesRef not
    String.  You can call the .utf8ToString() method on the BytesRef
    instances, if necessary.

## LUCENE-2600: IndexReaders are now read-only

  Instead of IndexReader.isDeleted, do this:

      import org.apache.lucene.util.Bits;
      import org.apache.lucene.index.MultiFields;

      Bits liveDocs = MultiFields.getLiveDocs(indexReader);
      if (liveDocs != null && !liveDocs.get(docID)) {
        // document is deleted...
      }
    
## LUCENE-2858, LUCENE-3733: IndexReader --> AtomicReader/CompositeReader/DirectoryReader refactoring

The abstract class IndexReader has been 
refactored to expose only essential methods to access stored fields 
during display of search results. It is no longer possible to retrieve 
terms or postings data from the underlying index, not even deletions are 
visible anymore. You can still pass IndexReader as constructor parameter 
to IndexSearcher and execute your searches; Lucene will automatically 
delegate procedures like query rewriting and document collection atomic 
subreaders. 

If you want to dive deeper into the index and want to write own queries, 
take a closer look at the new abstract sub-classes AtomicReader and 
CompositeReader: 

AtomicReader instances are now the only source of Terms, Postings, 
DocValues and FieldCache. Queries are forced to execute on a Atomic 
reader on a per-segment basis and FieldCaches are keyed by 
AtomicReaders. 

Its counterpart CompositeReader exposes a utility method to retrieve 
its composites. But watch out, composites are not necessarily atomic. 
Next to the added type-safety we also removed the notion of 
index-commits and version numbers from the abstract IndexReader, the 
associations with IndexWriter were pulled into a specialized 
DirectoryReader. To open Directory-based indexes use 
DirectoryReader.open(), the corresponding method in IndexReader is now 
deprecated for easier migration. Only DirectoryReader supports commits, 
versions, and reopening with openIfChanged(). Terms, postings, 
docvalues, and norms can from now on only be retrieved using 
AtomicReader; DirectoryReader and MultiReader extend CompositeReader, 
only offering stored fields and access to the sub-readers (which may be 
composite or atomic). 

If you have more advanced code dealing with custom Filters, you might 
have noticed another new class hierarchy in Lucene (see LUCENE-2831): 
IndexReaderContext with corresponding Atomic-/CompositeReaderContext. 

The move towards per-segment search Lucene 2.9 exposed lots of custom 
Queries and Filters that couldn't handle it. For example, some Filter 
implementations expected the IndexReader passed in is identical to the 
IndexReader passed to IndexSearcher with all its advantages like 
absolute document IDs etc. Obviously this "paradigm-shift" broke lots of 
applications and especially those that utilized cross-segment data 
structures (like Apache Solr). 

In Lucene 4.0, we introduce IndexReaderContexts "searcher-private" 
reader hierarchy. During Query or Filter execution Lucene no longer 
passes raw readers down Queries, Filters or Collectors; instead 
components are provided an AtomicReaderContext (essentially a hierarchy 
leaf) holding relative properties like the document-basis in relation to 
the top-level reader. This allows Queries & Filter to build up logic 
based on document IDs, albeit the per-segment orientation. 

There are still valid use-cases where top-level readers ie. "atomic 
views" on the index are desirable. Let say you want to iterate all terms 
of a complete index for auto-completion or faceting, Lucene provides
utility wrappers like SlowCompositeReaderWrapper (LUCENE-2597) emulating 
an AtomicReader. Note: using "atomicity emulators" can cause serious 
slowdowns due to the need to merge terms, postings, DocValues, and 
FieldCache, use them with care! 

## LUCENE-4306: getSequentialSubReaders(), ReaderUtil.Gather

The method IndexReader#getSequentialSubReaders() was moved to CompositeReader
(see LUCENE-2858, LUCENE-3733) and made protected. It is solely used by
CompositeReader itself to build its reader tree. To get all atomic leaves
of a reader, use IndexReader#leaves(), which also provides the doc base
of each leave. Readers that are already atomic return itself as leaf with
doc base 0. To emulate Lucene 3.x getSequentialSubReaders(),
use getContext().children().

## LUCENE-2413,LUCENE-3396: Analyzer package changes

Lucene's core and contrib analyzers, along with Solr's analyzers,
were consolidated into lucene/analysis. During the refactoring some
package names have changed, and ReusableAnalyzerBase was renamed to
Analyzer:

  - o.a.l.analysis.KeywordAnalyzer -> o.a.l.analysis.core.KeywordAnalyzer
  - o.a.l.analysis.KeywordTokenizer -> o.a.l.analysis.core.KeywordTokenizer
  - o.a.l.analysis.LetterTokenizer -> o.a.l.analysis.core.LetterTokenizer
  - o.a.l.analysis.LowerCaseFilter -> o.a.l.analysis.core.LowerCaseFilter
  - o.a.l.analysis.LowerCaseTokenizer -> o.a.l.analysis.core.LowerCaseTokenizer
  - o.a.l.analysis.SimpleAnalyzer -> o.a.l.analysis.core.SimpleAnalyzer
  - o.a.l.analysis.StopAnalyzer -> o.a.l.analysis.core.StopAnalyzer
  - o.a.l.analysis.StopFilter -> o.a.l.analysis.core.StopFilter
  - o.a.l.analysis.WhitespaceAnalyzer -> o.a.l.analysis.core.WhitespaceAnalyzer
  - o.a.l.analysis.WhitespaceTokenizer -> o.a.l.analysis.core.WhitespaceTokenizer
  - o.a.l.analysis.PorterStemFilter -> o.a.l.analysis.en.PorterStemFilter
  - o.a.l.analysis.ASCIIFoldingFilter -> o.a.l.analysis.miscellaneous.ASCIIFoldingFilter
  - o.a.l.analysis.ISOLatin1AccentFilter -> o.a.l.analysis.miscellaneous.ISOLatin1AccentFilter
  - o.a.l.analysis.KeywordMarkerFilter -> o.a.l.analysis.miscellaneous.KeywordMarkerFilter
  - o.a.l.analysis.LengthFilter -> o.a.l.analysis.miscellaneous.LengthFilter
  - o.a.l.analysis.PerFieldAnalyzerWrapper -> o.a.l.analysis.miscellaneous.PerFieldAnalyzerWrapper
  - o.a.l.analysis.TeeSinkTokenFilter -> o.a.l.analysis.sinks.TeeSinkTokenFilter
  - o.a.l.analysis.CharFilter -> o.a.l.analysis.charfilter.CharFilter
  - o.a.l.analysis.BaseCharFilter -> o.a.l.analysis.charfilter.BaseCharFilter
  - o.a.l.analysis.MappingCharFilter -> o.a.l.analysis.charfilter.MappingCharFilter
  - o.a.l.analysis.NormalizeCharMap -> o.a.l.analysis.charfilter.NormalizeCharMap
  - o.a.l.analysis.CharArraySet -> o.a.l.analysis.util.CharArraySet
  - o.a.l.analysis.CharArrayMap -> o.a.l.analysis.util.CharArrayMap
  - o.a.l.analysis.ReusableAnalyzerBase -> o.a.l.analysis.Analyzer
  - o.a.l.analysis.StopwordAnalyzerBase -> o.a.l.analysis.util.StopwordAnalyzerBase
  - o.a.l.analysis.WordListLoader -> o.a.l.analysis.util.WordListLoader
  - o.a.l.analysis.CharTokenizer -> o.a.l.analysis.util.CharTokenizer
  - o.a.l.util.CharacterUtils -> o.a.l.analysis.util.CharacterUtils

## LUCENE-2514: Collators

The option to use a Collator's order (instead of binary order) for
sorting and range queries has been moved to lucene/queries.
The Collated TermRangeQuery/Filter has been moved to SlowCollatedTermRangeQuery/Filter, 
and the collated sorting has been moved to SlowCollatedStringComparator.

Note: this functionality isn't very scalable and if you are using it, consider 
indexing collation keys with the collation support in the analysis module instead.

To perform collated range queries, use a suitable collating analyzer: CollationKeyAnalyzer 
or ICUCollationKeyAnalyzer, and set qp.setAnalyzeRangeTerms(true).

TermRangeQuery and TermRangeFilter now work purely on bytes. Both have helper factory methods
(newStringRange) similar to the NumericRange API, to easily perform range queries on Strings.

## LUCENE-2883: ValueSource changes

Lucene's o.a.l.search.function ValueSource based functionality, was consolidated
into lucene/queries along with Solr's similar functionality.  The following classes were moved:

 - o.a.l.search.function.CustomScoreQuery -> o.a.l.queries.CustomScoreQuery
 - o.a.l.search.function.CustomScoreProvider -> o.a.l.queries.CustomScoreProvider
 - o.a.l.search.function.NumericIndexDocValueSource -> o.a.l.queries.function.valuesource.NumericIndexDocValueSource

The following lists the replacement classes for those removed:

 - o.a.l.search.function.ByteFieldSource -> o.a.l.queries.function.valuesource.ByteFieldSource
 - o.a.l.search.function.DocValues -> o.a.l.queries.function.DocValues
 - o.a.l.search.function.FieldCacheSource -> o.a.l.queries.function.valuesource.FieldCacheSource
 - o.a.l.search.function.FieldScoreQuery ->o.a.l.queries.function.FunctionQuery
 - o.a.l.search.function.FloatFieldSource -> o.a.l.queries.function.valuesource.FloatFieldSource
 - o.a.l.search.function.IntFieldSource -> o.a.l.queries.function.valuesource.IntFieldSource
 - o.a.l.search.function.OrdFieldSource -> o.a.l.queries.function.valuesource.OrdFieldSource
 - o.a.l.search.function.ReverseOrdFieldSource -> o.a.l.queries.function.valuesource.ReverseOrdFieldSource
 - o.a.l.search.function.ShortFieldSource -> o.a.l.queries.function.valuesource.ShortFieldSource
 - o.a.l.search.function.ValueSource -> o.a.l.queries.function.ValueSource
 - o.a.l.search.function.ValueSourceQuery -> o.a.l.queries.function.FunctionQuery

DocValues are now named FunctionValues, to not confuse with Lucene's per-document values.

## LUCENE-2392: Enable flexible scoring

The existing "Similarity" api is now TFIDFSimilarity, if you were extending
Similarity before, you should likely extend this instead.

Weight.normalize no longer takes a norm value that incorporates the top-level
boost from outer queries such as BooleanQuery, instead it takes 2 parameters,
the outer boost (topLevelBoost) and the norm. Weight.sumOfSquaredWeights has
been renamed to Weight.getValueForNormalization().

The scorePayload method now takes a BytesRef. It is never null.

## LUCENE-3283: Query parsers moved to separate module

Lucene's core o.a.l.queryParser QueryParsers have been consolidated into lucene/queryparser,
where other QueryParsers from the codebase will also be placed.  The following classes were moved:

  - o.a.l.queryParser.CharStream -> o.a.l.queryparser.classic.CharStream
  - o.a.l.queryParser.FastCharStream -> o.a.l.queryparser.classic.FastCharStream
  - o.a.l.queryParser.MultiFieldQueryParser -> o.a.l.queryparser.classic.MultiFieldQueryParser
  - o.a.l.queryParser.ParseException -> o.a.l.queryparser.classic.ParseException
  - o.a.l.queryParser.QueryParser -> o.a.l.queryparser.classic.QueryParser
  - o.a.l.queryParser.QueryParserBase -> o.a.l.queryparser.classic.QueryParserBase
  - o.a.l.queryParser.QueryParserConstants -> o.a.l.queryparser.classic.QueryParserConstants
  - o.a.l.queryParser.QueryParserTokenManager -> o.a.l.queryparser.classic.QueryParserTokenManager
  - o.a.l.queryParser.QueryParserToken -> o.a.l.queryparser.classic.Token
  - o.a.l.queryParser.QueryParserTokenMgrError -> o.a.l.queryparser.classic.TokenMgrError

## LUCENE-2308, LUCENE-3453: Separate IndexableFieldType from Field instances

With this change, the indexing details (indexed, tokenized, norms,
indexOptions, stored, etc.) are moved into a separate FieldType
instance (rather than being stored directly on the Field).

This means you can create the FieldType instance once, up front,
for a given field, and then re-use that instance whenever you instantiate
the Field.

Certain field types are pre-defined since they are common cases:

  * StringField: indexes a String value as a single token (ie, does
    not tokenize).  This field turns off norms and indexes only doc
    IDS (does not index term frequency nor positions).  This field
    does not store its value, but exposes TYPE_STORED as well.
  * TextField: indexes and tokenizes a String, Reader or TokenStream
    value, without term vectors.  This field does not store its value,
    but exposes TYPE_STORED as well.
  * StoredField: field that stores its value
  * DocValuesField: indexes the value as a DocValues field
  * NumericField: indexes the numeric value so that NumericRangeQuery
    can be used at search-time.

If your usage fits one of those common cases you can simply
instantiate the above class.  If you need to store the value, you can
add a separate StoredField to the document, or you can use
TYPE_STORED for the field:

    Field f = new Field("field", "value", StringField.TYPE_STORED);

Alternatively, if an existing type is close to what you want but you
need to make a few changes, you can copy that type and make changes:

    FieldType bodyType = new FieldType(TextField.TYPE_STORED);
    bodyType.setStoreTermVectors(true);

You can of course also create your own FieldType from scratch:

    FieldType t = new FieldType();
    t.setIndexed(true);
    t.setStored(true);
    t.setOmitNorms(true);
    t.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
    t.freeze();

FieldType has a freeze() method to prevent further changes.

There is also a deprecated transition API, providing the same Index,
Store, TermVector enums from 3.x, and Field constructors taking these
enums.

When migrating from the 3.x API, if you did this before:

    new Field("field", "value", Field.Store.NO, Field.Indexed.NOT_ANALYZED_NO_NORMS)

you can now do this:

    new StringField("field", "value")

(though note that StringField indexes DOCS_ONLY).

If instead the value was stored:

    new Field("field", "value", Field.Store.YES, Field.Indexed.NOT_ANALYZED_NO_NORMS)

you can now do this:

    new Field("field", "value", StringField.TYPE_STORED)

If you didn't omit norms:

    new Field("field", "value", Field.Store.YES, Field.Indexed.NOT_ANALYZED)

you can now do this:

    FieldType ft = new FieldType(StringField.TYPE_STORED);
    ft.setOmitNorms(false);
    new Field("field", "value", ft)

If you did this before (value can be String or Reader):

    new Field("field", value, Field.Store.NO, Field.Indexed.ANALYZED)

you can now do this:

    new TextField("field", value, Field.Store.NO)

If instead the value was stored:

    new Field("field", value, Field.Store.YES, Field.Indexed.ANALYZED)

you can now do this:

    new TextField("field", value, Field.Store.YES)

If in addition you omit norms:

    new Field("field", value, Field.Store.YES, Field.Indexed.ANALYZED_NO_NORMS)

you can now do this:

    FieldType ft = new FieldType(TextField.TYPE_STORED);
    ft.setOmitNorms(true);
    new Field("field", value, ft)

If you did this before (bytes is a byte[]):

    new Field("field", bytes)

you can now do this:

    new StoredField("field", bytes)

If you previously used Document.setBoost, you must now pre-multiply
the document boost into each Field.setBoost.  If you have a
multi-valued field, you should do this only for the first Field
instance (ie, subsequent Field instance sharing the same field name
should only include their per-field boost and not the document level
boost) as the boost for multi-valued field instances are multiplied
together by Lucene.

## Other changes

* LUCENE-2674:
  A new idfExplain method was added to Similarity, that
  accepts an incoming docFreq.  If you subclass Similarity, make sure
  you also override this method on upgrade, otherwise your
  customizations won't run for certain MultiTermQuerys.

* LUCENE-2691: The near-real-time API has moved from IndexWriter to
  DirectoryReader.  Instead of IndexWriter.getReader(), call
  DirectoryReader.open(IndexWriter) or DirectoryReader.openIfChanged(IndexWriter).

* LUCENE-2690: MultiTermQuery boolean rewrites per segment.
  Also MultiTermQuery.getTermsEnum() now takes an AttributeSource. FuzzyTermsEnum
  is both consumer and producer of attributes: MTQ.BoostAttribute is
  added to the FuzzyTermsEnum and MTQ's rewrite mode consumes it.
  The other way round MTQ.TopTermsBooleanQueryRewrite supplies a
  global AttributeSource to each segments TermsEnum. The TermsEnum is consumer
  and gets the current minimum competitive boosts (MTQ.MaxNonCompetitiveBoostAttribute).

* LUCENE-2374: The backwards layer in AttributeImpl was removed. To support correct
  reflection of AttributeImpl instances, where the reflection was done using deprecated
  toString() parsing, you have to now override reflectWith() to customize output.
  toString() is no longer implemented by AttributeImpl, so if you have overridden
  toString(), port your customization over to reflectWith(). reflectAsString() would
  then return what toString() did before.

* LUCENE-2236, LUCENE-2912: DefaultSimilarity can no longer be set statically 
  (and dangerously) for the entire JVM.
  Similarity can now be configured on a per-field basis (via PerFieldSimilarityWrapper)
  Similarity has a lower-level API, if you want the higher-level vector-space API
  like in previous Lucene releases, then look at TFIDFSimilarity.

* LUCENE-1076: TieredMergePolicy is now the default merge policy.
  It's able to merge non-contiguous segments; this may cause problems
  for applications that rely on Lucene's internal document ID
  assignment.  If so, you should instead use LogByteSize/DocMergePolicy
  during indexing.

* LUCENE-3722: Similarity methods and collection/term statistics now take
  long instead of int (to enable distributed scoring of > 2B docs). 
  For example, in TFIDFSimilarity idf(int, int) is now idf(long, long). 

* LUCENE-3559: The methods "docFreq" and "maxDoc" on IndexSearcher were removed,
  as these are no longer used by the scoring system.
  If you were using these casually in your code for reasons unrelated to scoring,
  call them on the IndexSearcher's reader instead: getIndexReader().
  If you were subclassing IndexSearcher and overriding these methods to alter
  scoring, override IndexSearcher's termStatistics() and collectionStatistics()
  methods instead.

* LUCENE-3396: Analyzer.tokenStream() and .reusableTokenStream() have been made final.
  It is now necessary to use Analyzer.TokenStreamComponents to define an analysis process.
  Analyzer also has its own way of managing the reuse of TokenStreamComponents (either
  globally, or per-field).  To define another Strategy, implement Analyzer.ReuseStrategy.

* LUCENE-3464: IndexReader.reopen has been renamed to
  DirectoryReader.openIfChanged (a static method), and now returns null
  (instead of the old reader) if there are no changes to the index, to
  prevent the common pitfall of accidentally closing the old reader.
  
* LUCENE-3687: Similarity#computeNorm() now expects a Norm object to set the computed 
  norm value instead of returning a fixed single byte value. Custom similarities can now
  set integer, float and byte values if a single byte is not sufficient.

* LUCENE-2621: Term vectors are now accessed via flexible indexing API.
  If you used IndexReader.getTermFreqVector/s before, you should now
  use IndexReader.getTermVectors.  The new method returns a Fields
  instance exposing the inverted index of the one document.  From
  Fields you can enumerate all fields, terms, positions, offsets.

* LUCENE-4227: If you were previously using Instantiated index, you
  may want to use DirectPostingsFormat after upgrading: it stores all
  postings in simple arrrays (byte[] for terms, int[] for docs, freqs,
  positions, offsets).  Note that this only covers postings, whereas
  Instantiated covered all other parts of the index as well.

* LUCENE-3309: The expert FieldSelector API has been replaced with
  StoredFieldVisitor.  The idea is the same (you have full control
  over which fields should be loaded).  Instead of a single accept
  method, StoredFieldVisitor has a needsField method: if that method
  returns true then the field will be loaded and the appropriate
  type-specific method will be invoked with that fields's value.

* LUCENE-4122: Removed the Payload class and replaced with BytesRef.
  PayloadAttribute's name is unchanged, it just uses the BytesRef
  class to refer to the payload bytes/start offset/end offset 
  (or null if there is no payload).
