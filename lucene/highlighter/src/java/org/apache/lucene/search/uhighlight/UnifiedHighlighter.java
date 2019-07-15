/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.search.uhighlight;

import java.io.IOException;
import java.text.BreakIterator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.BaseCompositeReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.InPlaceMergeSorter;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;

/**
 * A Highlighter that can get offsets from either
 * postings ({@link IndexOptions#DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS}),
 * term vectors ({@link FieldType#setStoreTermVectorOffsets(boolean)}),
 * or via re-analyzing text.
 * <p>
 * This highlighter treats the single original document as the whole corpus, and then scores individual
 * passages as if they were documents in this corpus. It uses a {@link BreakIterator} to find
 * passages in the text; by default it breaks using {@link BreakIterator#getSentenceInstance(Locale)
 * getSentenceInstance(Locale.ROOT)}. It then iterates in parallel (merge sorting by offset) through
 * the positions of all terms from the query, coalescing those hits that occur in a single passage
 * into a {@link Passage}, and then scores each Passage using a separate {@link PassageScorer}.
 * Passages are finally formatted into highlighted snippets with a {@link PassageFormatter}.
 * <p>
 * You can customize the behavior by calling some of the setters, or by subclassing and overriding some methods.
 * Some important hooks:
 * <ul>
 * <li>{@link #getBreakIterator(String)}: Customize how the text is divided into passages.
 * <li>{@link #getScorer(String)}: Customize how passages are ranked.
 * <li>{@link #getFormatter(String)}: Customize how snippets are formatted.
 * </ul>
 * <p>
 * This is thread-safe.
 *
 * @lucene.experimental
 */
public class UnifiedHighlighter {

  protected static final char MULTIVAL_SEP_CHAR = (char) 0;

  public static final int DEFAULT_MAX_LENGTH = 10000;

  public static final int DEFAULT_CACHE_CHARS_THRESHOLD = 524288; // ~ 1 MB (2 byte chars)

  static final IndexSearcher EMPTY_INDEXSEARCHER;

  static {
    try {
      IndexReader emptyReader = new MultiReader();
      EMPTY_INDEXSEARCHER = new IndexSearcher(emptyReader);
      EMPTY_INDEXSEARCHER.setQueryCache(null);
    } catch (IOException bogus) {
      throw new RuntimeException(bogus);
    }
  }

  protected static final CharacterRunAutomaton[] ZERO_LEN_AUTOMATA_ARRAY = new CharacterRunAutomaton[0];

  protected final IndexSearcher searcher; // if null, can only use highlightWithoutSearcher

  protected final Analyzer indexAnalyzer;

  private boolean defaultHandleMtq = true; // e.g. wildcards

  private boolean defaultHighlightPhrasesStrictly = true; // AKA "accuracy" or "query debugging"

  private boolean defaultPassageRelevancyOverSpeed = true; //For analysis, prefer MemoryIndexOffsetStrategy

  private int maxLength = DEFAULT_MAX_LENGTH;

  // BreakIterator is stateful so we use a Supplier factory method
  private Supplier<BreakIterator> defaultBreakIterator = () -> BreakIterator.getSentenceInstance(Locale.ROOT);

  private Predicate<String> defaultFieldMatcher;

  private PassageScorer defaultScorer = new PassageScorer();

  private PassageFormatter defaultFormatter = new DefaultPassageFormatter();

  private int defaultMaxNoHighlightPassages = -1;

  // lazy initialized with double-check locking; protected so subclass can init
  protected volatile FieldInfos fieldInfos;

  private int cacheFieldValCharsThreshold = DEFAULT_CACHE_CHARS_THRESHOLD;

  /**
   * Extracts matching terms after rewriting against an empty index
   */
  protected static Set<Term> extractTerms(Query query) throws IOException {
    Set<Term> queryTerms = new HashSet<>();
    EMPTY_INDEXSEARCHER.rewrite(query).visit(QueryVisitor.termCollector(queryTerms));
    return queryTerms;
  }

  /**
   * Constructs the highlighter with the given index searcher and analyzer.
   *
   * @param indexSearcher Usually required, unless {@link #highlightWithoutSearcher(String, Query, String, int)} is
   *                      used, in which case this needs to be null.
   * @param indexAnalyzer Required, even if in some circumstances it isn't used.
   */
  public UnifiedHighlighter(IndexSearcher indexSearcher, Analyzer indexAnalyzer) {
    this.searcher = indexSearcher; //TODO: make non nullable
    this.indexAnalyzer = Objects.requireNonNull(indexAnalyzer,
        "indexAnalyzer is required"
            + " (even if in some circumstances it isn't used)");
  }

  public void setHandleMultiTermQuery(boolean handleMtq) {
    this.defaultHandleMtq = handleMtq;
  }

  public void setHighlightPhrasesStrictly(boolean highlightPhrasesStrictly) {
    this.defaultHighlightPhrasesStrictly = highlightPhrasesStrictly;
  }

  public void setMaxLength(int maxLength) {
    if (maxLength < 0 || maxLength == Integer.MAX_VALUE) {
      // two reasons: no overflow problems in BreakIterator.preceding(offset+1),
      // our sentinel in the offsets queue uses this value to terminate.
      throw new IllegalArgumentException("maxLength must be < Integer.MAX_VALUE");
    }
    this.maxLength = maxLength;
  }

  public void setBreakIterator(Supplier<BreakIterator> breakIterator) {
    this.defaultBreakIterator = breakIterator;
  }

  public void setScorer(PassageScorer scorer) {
    this.defaultScorer = scorer;
  }

  public void setFormatter(PassageFormatter formatter) {
    this.defaultFormatter = formatter;
  }

  public void setMaxNoHighlightPassages(int defaultMaxNoHighlightPassages) {
    this.defaultMaxNoHighlightPassages = defaultMaxNoHighlightPassages;
  }

  public void setCacheFieldValCharsThreshold(int cacheFieldValCharsThreshold) {
    this.cacheFieldValCharsThreshold = cacheFieldValCharsThreshold;
  }

  public void setFieldMatcher(Predicate<String> predicate) {
    this.defaultFieldMatcher = predicate;
  }

  /**
   * Returns whether {@link MultiTermQuery} derivatives will be highlighted.  By default it's enabled.  MTQ
   * highlighting can be expensive, particularly when using offsets in postings.
   */
  protected boolean shouldHandleMultiTermQuery(String field) {
    return defaultHandleMtq;
  }

  /**
   * Returns whether position sensitive queries (e.g. phrases and {@link SpanQuery}ies)
   * should be highlighted strictly based on query matches (slower)
   * versus any/all occurrences of the underlying terms.  By default it's enabled, but there's no overhead if such
   * queries aren't used.
   */
  protected boolean shouldHighlightPhrasesStrictly(String field) {
    return defaultHighlightPhrasesStrictly;
  }


  protected boolean shouldPreferPassageRelevancyOverSpeed(String field) {
    return defaultPassageRelevancyOverSpeed;
  }

  /**
   * Returns the predicate to use for extracting the query part that must be highlighted.
   * By default only queries that target the current field are kept. (AKA requireFieldMatch)
   */
  protected Predicate<String> getFieldMatcher(String field) {
    if (defaultFieldMatcher != null) {
      return defaultFieldMatcher;
    } else {
      // requireFieldMatch = true
      return (qf) -> field.equals(qf);
    }
  }

  /**
   * The maximum content size to process.  Content will be truncated to this size before highlighting. Typically
   * snippets closer to the beginning of the document better summarize its content.
   */
  public int getMaxLength() {
    return maxLength;
  }

  /**
   * Returns the {@link BreakIterator} to use for
   * dividing text into passages.  This returns
   * {@link BreakIterator#getSentenceInstance(Locale)} by default;
   * subclasses can override to customize.
   * <p>
   * Note: this highlighter will call
   * {@link BreakIterator#preceding(int)} and {@link BreakIterator#next()} many times on it.
   * The default generic JDK implementation of {@code preceding} performs poorly.
   */
  protected BreakIterator getBreakIterator(String field) {
    return defaultBreakIterator.get();
  }

  /**
   * Returns the {@link PassageScorer} to use for
   * ranking passages.  This
   * returns a new {@code PassageScorer} by default;
   * subclasses can override to customize.
   */
  protected PassageScorer getScorer(String field) {
    return defaultScorer;
  }

  /**
   * Returns the {@link PassageFormatter} to use for
   * formatting passages into highlighted snippets.  This
   * returns a new {@code PassageFormatter} by default;
   * subclasses can override to customize.
   */
  protected PassageFormatter getFormatter(String field) {
    return defaultFormatter;
  }

  /**
   * Returns the number of leading passages (as delineated by the {@link BreakIterator}) when no
   * highlights could be found.  If it's less than 0 (the default) then this defaults to the {@code maxPassages}
   * parameter given for each request.  If this is 0 then the resulting highlight is null (not formatted).
   */
  protected int getMaxNoHighlightPassages(String field) {
    return defaultMaxNoHighlightPassages;
  }

  /**
   * Limits the amount of field value pre-fetching until this threshold is passed.  The highlighter
   * internally highlights in batches of documents sized on the sum field value length (in chars) of the fields
   * to be highlighted (bounded by {@link #getMaxLength()} for each field).  By setting this to 0, you can force
   * documents to be fetched and highlighted one at a time, which you usually shouldn't do.
   * The default is 524288 chars which translates to about a megabyte.  However, note
   * that the highlighter sometimes ignores this and highlights one document at a time (without caching a
   * bunch of documents in advance) when it can detect there's no point in it -- such as when all fields will be
   * highlighted via re-analysis as one example.
   */
  public int getCacheFieldValCharsThreshold() { // question: should we size by bytes instead?
    return cacheFieldValCharsThreshold;
  }

  /**
   * ... as passed in from constructor.
   */
  public IndexSearcher getIndexSearcher() {
    return searcher;
  }

  /**
   * ... as passed in from constructor.
   */
  public Analyzer getIndexAnalyzer() {
    return indexAnalyzer;
  }

  /**
   * Source of term offsets; essential for highlighting.
   */
  public enum OffsetSource {
    POSTINGS, TERM_VECTORS, ANALYSIS, POSTINGS_WITH_TERM_VECTORS, NONE_NEEDED
  }

  /**
   * Determine the offset source for the specified field.  The default algorithm is as follows:
   * <ol>
   * <li>This calls {@link #getFieldInfo(String)}. Note this returns null if there is no searcher or if the
   * field isn't found there.</li>
   * <li> If there's a field info it has
   * {@link IndexOptions#DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS} then {@link OffsetSource#POSTINGS} is
   * returned.</li>
   * <li>If there's a field info and {@link FieldInfo#hasVectors()} then {@link OffsetSource#TERM_VECTORS} is
   * returned (note we can't check here if the TV has offsets; if there isn't then an exception will get thrown
   * down the line).</li>
   * <li>Fall-back: {@link OffsetSource#ANALYSIS} is returned.</li>
   * </ol>
   * <p>
   * Note that the highlighter sometimes switches to something else based on the query, such as if you have
   * {@link OffsetSource#POSTINGS_WITH_TERM_VECTORS} but in fact don't need term vectors.
   */
  protected OffsetSource getOffsetSource(String field) {
    FieldInfo fieldInfo = getFieldInfo(field);
    if (fieldInfo != null) {
      if (fieldInfo.getIndexOptions() == IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) {
        return fieldInfo.hasVectors() ? OffsetSource.POSTINGS_WITH_TERM_VECTORS : OffsetSource.POSTINGS;
      }
      if (fieldInfo.hasVectors()) { // unfortunately we can't also check if the TV has offsets
        return OffsetSource.TERM_VECTORS;
      }
    }
    return OffsetSource.ANALYSIS;
  }

  /**
   * Called by the default implementation of {@link #getOffsetSource(String)}.
   * If there is no searcher then we simply always return null.
   */
  protected FieldInfo getFieldInfo(String field) {
    if (searcher == null) {
      return null;
    }
    // Need thread-safety for lazy-init but lets avoid 'synchronized' by using double-check locking idiom
    FieldInfos fieldInfos = this.fieldInfos; // note: it's volatile; read once
    if (fieldInfos == null) {
      synchronized (this) {
        fieldInfos = this.fieldInfos;
        if (fieldInfos == null) {
          fieldInfos = FieldInfos.getMergedFieldInfos(searcher.getIndexReader());
          this.fieldInfos = fieldInfos;
        }

      }

    }
    return fieldInfos.fieldInfo(field);
  }

  /**
   * Highlights the top passages from a single field.
   *
   * @param field   field name to highlight.
   *                Must have a stored string value and also be indexed with offsets.
   * @param query   query to highlight.
   * @param topDocs TopDocs containing the summary result documents to highlight.
   * @return Array of formatted snippets corresponding to the documents in <code>topDocs</code>.
   * If no highlights were found for a document, the
   * first sentence for the field will be returned.
   * @throws IOException              if an I/O error occurred during processing
   * @throws IllegalArgumentException if <code>field</code> was indexed without
   *                                  {@link IndexOptions#DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS}
   */
  public String[] highlight(String field, Query query, TopDocs topDocs) throws IOException {
    return highlight(field, query, topDocs, 1);
  }

  /**
   * Highlights the top-N passages from a single field.
   *
   * @param field       field name to highlight. Must have a stored string value.
   * @param query       query to highlight.
   * @param topDocs     TopDocs containing the summary result documents to highlight.
   * @param maxPassages The maximum number of top-N ranked passages used to
   *                    form the highlighted snippets.
   * @return Array of formatted snippets corresponding to the documents in <code>topDocs</code>.
   * If no highlights were found for a document, the
   * first {@code maxPassages} sentences from the
   * field will be returned.
   * @throws IOException              if an I/O error occurred during processing
   * @throws IllegalArgumentException if <code>field</code> was indexed without
   *                                  {@link IndexOptions#DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS}
   */
  public String[] highlight(String field, Query query, TopDocs topDocs, int maxPassages) throws IOException {
    Map<String, String[]> res = highlightFields(new String[]{field}, query, topDocs, new int[]{maxPassages});
    return res.get(field);
  }

  /**
   * Highlights the top passages from multiple fields.
   * <p>
   * Conceptually, this behaves as a more efficient form of:
   * <pre class="prettyprint">
   * Map m = new HashMap();
   * for (String field : fields) {
   * m.put(field, highlight(field, query, topDocs));
   * }
   * return m;
   * </pre>
   *
   * @param fields  field names to highlight. Must have a stored string value.
   * @param query   query to highlight.
   * @param topDocs TopDocs containing the summary result documents to highlight.
   * @return Map keyed on field name, containing the array of formatted snippets
   * corresponding to the documents in <code>topDocs</code>.
   * If no highlights were found for a document, the
   * first sentence from the field will be returned.
   * @throws IOException              if an I/O error occurred during processing
   * @throws IllegalArgumentException if <code>field</code> was indexed without
   *                                  {@link IndexOptions#DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS}
   */
  public Map<String, String[]> highlightFields(String[] fields, Query query, TopDocs topDocs) throws IOException {
    int maxPassages[] = new int[fields.length];
    Arrays.fill(maxPassages, 1);
    return highlightFields(fields, query, topDocs, maxPassages);
  }

  /**
   * Highlights the top-N passages from multiple fields.
   * <p>
   * Conceptually, this behaves as a more efficient form of:
   * <pre class="prettyprint">
   * Map m = new HashMap();
   * for (String field : fields) {
   * m.put(field, highlight(field, query, topDocs, maxPassages));
   * }
   * return m;
   * </pre>
   *
   * @param fields      field names to highlight. Must have a stored string value.
   * @param query       query to highlight.
   * @param topDocs     TopDocs containing the summary result documents to highlight.
   * @param maxPassages The maximum number of top-N ranked passages per-field used to
   *                    form the highlighted snippets.
   * @return Map keyed on field name, containing the array of formatted snippets
   * corresponding to the documents in <code>topDocs</code>.
   * If no highlights were found for a document, the
   * first {@code maxPassages} sentences from the
   * field will be returned.
   * @throws IOException              if an I/O error occurred during processing
   * @throws IllegalArgumentException if <code>field</code> was indexed without
   *                                  {@link IndexOptions#DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS}
   */
  public Map<String, String[]> highlightFields(String[] fields, Query query, TopDocs topDocs, int[] maxPassages)
      throws IOException {
    final ScoreDoc scoreDocs[] = topDocs.scoreDocs;
    int docids[] = new int[scoreDocs.length];
    for (int i = 0; i < docids.length; i++) {
      docids[i] = scoreDocs[i].doc;
    }

    return highlightFields(fields, query, docids, maxPassages);
  }

  /**
   * Highlights the top-N passages from multiple fields,
   * for the provided int[] docids.
   *
   * @param fieldsIn      field names to highlight. Must have a stored string value.
   * @param query         query to highlight.
   * @param docidsIn      containing the document IDs to highlight.
   * @param maxPassagesIn The maximum number of top-N ranked passages per-field used to
   *                      form the highlighted snippets.
   * @return Map keyed on field name, containing the array of formatted snippets
   * corresponding to the documents in <code>docidsIn</code>.
   * If no highlights were found for a document, the
   * first {@code maxPassages} from the field will
   * be returned.
   * @throws IOException              if an I/O error occurred during processing
   * @throws IllegalArgumentException if <code>field</code> was indexed without
   *                                  {@link IndexOptions#DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS}
   */
  public Map<String, String[]> highlightFields(String[] fieldsIn, Query query, int[] docidsIn, int[] maxPassagesIn)
      throws IOException {
    Map<String, String[]> snippets = new HashMap<>();
    for (Map.Entry<String, Object[]> ent : highlightFieldsAsObjects(fieldsIn, query, docidsIn, maxPassagesIn).entrySet()) {
      Object[] snippetObjects = ent.getValue();
      String[] snippetStrings = new String[snippetObjects.length];
      snippets.put(ent.getKey(), snippetStrings);
      for (int i = 0; i < snippetObjects.length; i++) {
        Object snippet = snippetObjects[i];
        if (snippet != null) {
          snippetStrings[i] = snippet.toString();
        }
      }
    }

    return snippets;
  }

  /**
   * Expert: highlights the top-N passages from multiple fields,
   * for the provided int[] docids, to custom Object as
   * returned by the {@link PassageFormatter}.  Use
   * this API to render to something other than String.
   *
   * @param fieldsIn      field names to highlight. Must have a stored string value.
   * @param query         query to highlight.
   * @param docIdsIn      containing the document IDs to highlight.
   * @param maxPassagesIn The maximum number of top-N ranked passages per-field used to
   *                      form the highlighted snippets.
   * @return Map keyed on field name, containing the array of formatted snippets
   * corresponding to the documents in <code>docIdsIn</code>.
   * If no highlights were found for a document, the
   * first {@code maxPassages} from the field will
   * be returned.
   * @throws IOException              if an I/O error occurred during processing
   * @throws IllegalArgumentException if <code>field</code> was indexed without
   *                                  {@link IndexOptions#DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS}
   */
  protected Map<String, Object[]> highlightFieldsAsObjects(String[] fieldsIn, Query query, int[] docIdsIn,
                                                           int[] maxPassagesIn) throws IOException {
    if (fieldsIn.length < 1) {
      throw new IllegalArgumentException("fieldsIn must not be empty");
    }
    if (fieldsIn.length != maxPassagesIn.length) {
      throw new IllegalArgumentException("invalid number of maxPassagesIn");
    }
    if (searcher == null) {
      throw new IllegalStateException("This method requires that an indexSearcher was passed in the "
          + "constructor.  Perhaps you mean to call highlightWithoutSearcher?");
    }

    // Sort docs & fields for sequential i/o

    // Sort doc IDs w/ index to original order: (copy input arrays since we sort in-place)
    int[] docIds = new int[docIdsIn.length];
    int[] docInIndexes = new int[docIds.length]; // fill in ascending order; points into docIdsIn[]
    copyAndSortDocIdsWithIndex(docIdsIn, docIds, docInIndexes); // latter 2 are "out" params

    // Sort fields w/ maxPassages pair: (copy input arrays since we sort in-place)
    final String fields[] = new String[fieldsIn.length];
    final int maxPassages[] = new int[maxPassagesIn.length];
    copyAndSortFieldsWithMaxPassages(fieldsIn, maxPassagesIn, fields, maxPassages); // latter 2 are "out" params

    // Init field highlighters (where most of the highlight logic lives, and on a per field basis)
    Set<Term> queryTerms = extractTerms(query);
    FieldHighlighter[] fieldHighlighters = new FieldHighlighter[fields.length];
    int numTermVectors = 0;
    int numPostings = 0;
    for (int f = 0; f < fields.length; f++) {
      FieldHighlighter fieldHighlighter = getFieldHighlighter(fields[f], query, queryTerms, maxPassages[f]);
      fieldHighlighters[f] = fieldHighlighter;

      switch (fieldHighlighter.getOffsetSource()) {
        case TERM_VECTORS:
          numTermVectors++;
          break;
        case POSTINGS:
          numPostings++;
          break;
        case POSTINGS_WITH_TERM_VECTORS:
          numTermVectors++;
          numPostings++;
          break;
        case ANALYSIS:
        case NONE_NEEDED:
        default:
          //do nothing
          break;
      }
    }

    int cacheCharsThreshold = calculateOptimalCacheCharsThreshold(numTermVectors, numPostings);

    IndexReader indexReaderWithTermVecCache =
        (numTermVectors >= 2) ? TermVectorReusingLeafReader.wrap(searcher.getIndexReader()) : null;

    // [fieldIdx][docIdInIndex] of highlightDoc result
    Object[][] highlightDocsInByField = new Object[fields.length][docIds.length];
    // Highlight in doc batches determined by loadFieldValues (consumes from docIdIter)
    DocIdSetIterator docIdIter = asDocIdSetIterator(docIds);
    for (int batchDocIdx = 0; batchDocIdx < docIds.length; ) {
      // Load the field values of the first batch of document(s) (note: commonly all docs are in this batch)
      List<CharSequence[]> fieldValsByDoc =
          loadFieldValues(fields, docIdIter, cacheCharsThreshold);
      //    the size of the above list is the size of the batch (num of docs in the batch)

      // Highlight in per-field order first, then by doc (better I/O pattern)
      for (int fieldIdx = 0; fieldIdx < fields.length; fieldIdx++) {
        Object[] resultByDocIn = highlightDocsInByField[fieldIdx];//parallel to docIdsIn
        FieldHighlighter fieldHighlighter = fieldHighlighters[fieldIdx];
        for (int docIdx = batchDocIdx; docIdx - batchDocIdx < fieldValsByDoc.size(); docIdx++) {
          int docId = docIds[docIdx];//sorted order
          CharSequence content = fieldValsByDoc.get(docIdx - batchDocIdx)[fieldIdx];
          if (content == null) {
            continue;
          }
          IndexReader indexReader =
              (fieldHighlighter.getOffsetSource() == OffsetSource.TERM_VECTORS
                  && indexReaderWithTermVecCache != null)
                  ? indexReaderWithTermVecCache
                  : searcher.getIndexReader();
          final LeafReader leafReader;
          if (indexReader instanceof LeafReader) {
            leafReader = (LeafReader) indexReader;
          } else {
            List<LeafReaderContext> leaves = indexReader.leaves();
            LeafReaderContext leafReaderContext = leaves.get(ReaderUtil.subIndex(docId, leaves));
            leafReader = leafReaderContext.reader();
            docId -= leafReaderContext.docBase; // adjust 'doc' to be within this leaf reader
          }
          int docInIndex = docInIndexes[docIdx];//original input order
          assert resultByDocIn[docInIndex] == null;
          resultByDocIn[docInIndex] =
              fieldHighlighter
                  .highlightFieldForDoc(leafReader, docId, content.toString());
        }

      }

      batchDocIdx += fieldValsByDoc.size();
    }
    assert docIdIter.docID() == DocIdSetIterator.NO_MORE_DOCS
        || docIdIter.nextDoc() == DocIdSetIterator.NO_MORE_DOCS;

    // TODO reconsider the return type; since this is an "advanced" method, lets not return a Map?  Notice the only
    //    caller simply iterates it to build another structure.

    // field -> object highlights parallel to docIdsIn
    Map<String, Object[]> resultMap = new HashMap<>(fields.length);
    for (int f = 0; f < fields.length; f++) {
      resultMap.put(fields[f], highlightDocsInByField[f]);
    }
    return resultMap;
  }

  /**
   * When cacheCharsThreshold is 0, loadFieldValues() only fetches one document at a time.  We override it to be 0
   * in two circumstances:
   */
  private int calculateOptimalCacheCharsThreshold(int numTermVectors, int numPostings) {
    if (numPostings == 0 && numTermVectors == 0) {
      // (1) When all fields are ANALYSIS there's no point in caching a batch of documents
      // because no other info on disk is needed to highlight it.
      return 0;
    } else if (numTermVectors >= 2) {
      // (2) When two or more fields have term vectors, given the field-then-doc algorithm, the underlying term
      // vectors will be fetched in a terrible access pattern unless we highlight a doc at a time and use a special
      // current-doc TV cache.  So we do that.  Hopefully one day TVs will be improved to make this pointless.
      return 0;
    } else {
      return getCacheFieldValCharsThreshold();
    }
  }

  private void copyAndSortFieldsWithMaxPassages(String[] fieldsIn, int[] maxPassagesIn, final String[] fields,
                                                final int[] maxPassages) {
    System.arraycopy(fieldsIn, 0, fields, 0, fieldsIn.length);
    System.arraycopy(maxPassagesIn, 0, maxPassages, 0, maxPassagesIn.length);
    new InPlaceMergeSorter() {
      @Override
      protected void swap(int i, int j) {
        String tmp = fields[i];
        fields[i] = fields[j];
        fields[j] = tmp;
        int tmp2 = maxPassages[i];
        maxPassages[i] = maxPassages[j];
        maxPassages[j] = tmp2;
      }

      @Override
      protected int compare(int i, int j) {
        return fields[i].compareTo(fields[j]);
      }

    }.sort(0, fields.length);
  }

  private void copyAndSortDocIdsWithIndex(int[] docIdsIn, final int[] docIds, final int[] docInIndexes) {
    System.arraycopy(docIdsIn, 0, docIds, 0, docIdsIn.length);
    for (int i = 0; i < docInIndexes.length; i++) {
      docInIndexes[i] = i;
    }
    new InPlaceMergeSorter() {
      @Override
      protected void swap(int i, int j) {
        int tmp = docIds[i];
        docIds[i] = docIds[j];
        docIds[j] = tmp;
        tmp = docInIndexes[i];
        docInIndexes[i] = docInIndexes[j];
        docInIndexes[j] = tmp;
      }

      @Override
      protected int compare(int i, int j) {
        return Integer.compare(docIds[i], docIds[j]);
      }
    }.sort(0, docIds.length);
  }

  /**
   * Highlights text passed as a parameter.  This requires the {@link IndexSearcher} provided to this highlighter is
   * null.  This use-case is more rare.  Naturally, the mode of operation will be {@link OffsetSource#ANALYSIS}.
   * The result of this method is whatever the {@link PassageFormatter} returns.  For the {@link
   * DefaultPassageFormatter} and assuming {@code content} has non-zero length, the result will be a non-null
   * string -- so it's safe to call {@link Object#toString()} on it in that case.
   *
   * @param field       field name to highlight (as found in the query).
   * @param query       query to highlight.
   * @param content     text to highlight.
   * @param maxPassages The maximum number of top-N ranked passages used to
   *                    form the highlighted snippets.
   * @return result of the {@link PassageFormatter} -- probably a String.  Might be null.
   * @throws IOException if an I/O error occurred during processing
   */
  //TODO make content a List? and return a List? and ensure getEmptyHighlight is never invoked multiple times?
  public Object highlightWithoutSearcher(String field, Query query, String content, int maxPassages)
      throws IOException {
    if (this.searcher != null) {
      throw new IllegalStateException("highlightWithoutSearcher should only be called on a " +
          getClass().getSimpleName() + " without an IndexSearcher.");
    }
    Objects.requireNonNull(content, "content is required");
    Set<Term> queryTerms = extractTerms(query);
    return getFieldHighlighter(field, query, queryTerms, maxPassages)
        .highlightFieldForDoc(null, -1, content);
  }

  protected FieldHighlighter getFieldHighlighter(String field, Query query, Set<Term> allTerms, int maxPassages) {
    UHComponents components = getHighlightComponents(field, query, allTerms);
    OffsetSource offsetSource = getOptimizedOffsetSource(components);
    return new FieldHighlighter(field,
        getOffsetStrategy(offsetSource, components),
        new SplittingBreakIterator(getBreakIterator(field), UnifiedHighlighter.MULTIVAL_SEP_CHAR),
        getScorer(field),
        maxPassages,
        getMaxNoHighlightPassages(field),
        getFormatter(field));
  }

  protected UHComponents getHighlightComponents(String field, Query query, Set<Term> allTerms) {
    Predicate<String> fieldMatcher = getFieldMatcher(field);
    Set<HighlightFlag> highlightFlags = getFlags(field);
    PhraseHelper phraseHelper = getPhraseHelper(field, query, highlightFlags);
    boolean queryHasUnrecognizedPart = hasUnrecognizedQuery(fieldMatcher, query);
    BytesRef[] terms = null;
    CharacterRunAutomaton[] automata = null;
    if (!highlightFlags.contains(HighlightFlag.WEIGHT_MATCHES) || !queryHasUnrecognizedPart) {
      terms = filterExtractedTerms(fieldMatcher, allTerms);
      automata = getAutomata(field, query, highlightFlags);
    } // otherwise don't need to extract
    return new UHComponents(field, fieldMatcher, query, terms, phraseHelper, automata, queryHasUnrecognizedPart, highlightFlags);
  }

  protected boolean hasUnrecognizedQuery(Predicate<String> fieldMatcher, Query query) {
    boolean[] hasUnknownLeaf = new boolean[1];
    query.visit(new QueryVisitor() {
      @Override
      public boolean acceptField(String field) {
        // checking hasUnknownLeaf is a trick to exit early
        return hasUnknownLeaf[0] == false && fieldMatcher.test(field);
      }

      @Override
      public void visitLeaf(Query query) {
        if (MultiTermHighlighting.canExtractAutomataFromLeafQuery(query) == false) {
          if (!(query instanceof MatchAllDocsQuery || query instanceof MatchNoDocsQuery)) {
            hasUnknownLeaf[0] = true;
          }
        }
      }
    });
    return hasUnknownLeaf[0];
  }

  protected static BytesRef[] filterExtractedTerms(Predicate<String> fieldMatcher, Set<Term> queryTerms) {
    // Strip off the redundant field and sort the remaining terms
    SortedSet<BytesRef> filteredTerms = new TreeSet<>();
    for (Term term : queryTerms) {
      if (fieldMatcher.test(term.field())) {
        filteredTerms.add(term.bytes());
      }
    }
    return filteredTerms.toArray(new BytesRef[filteredTerms.size()]);
  }

  protected Set<HighlightFlag> getFlags(String field) {
    Set<HighlightFlag> highlightFlags = EnumSet.noneOf(HighlightFlag.class);
    if (shouldHandleMultiTermQuery(field)) {
      highlightFlags.add(HighlightFlag.MULTI_TERM_QUERY);
    }
    if (shouldHighlightPhrasesStrictly(field)) {
      highlightFlags.add(HighlightFlag.PHRASES);
    }
    if (shouldPreferPassageRelevancyOverSpeed(field)) {
      highlightFlags.add(HighlightFlag.PASSAGE_RELEVANCY_OVER_SPEED);
    }
    return highlightFlags;
  }

  protected PhraseHelper getPhraseHelper(String field, Query query, Set<HighlightFlag> highlightFlags) {
    boolean useWeightMatchesIter = highlightFlags.contains(HighlightFlag.WEIGHT_MATCHES);
    if (useWeightMatchesIter) {
      return PhraseHelper.NONE; // will be handled by Weight.matches which always considers phrases
    }
    boolean highlightPhrasesStrictly = highlightFlags.contains(HighlightFlag.PHRASES);
    boolean handleMultiTermQuery = highlightFlags.contains(HighlightFlag.MULTI_TERM_QUERY);
    return highlightPhrasesStrictly ?
        new PhraseHelper(query, field, getFieldMatcher(field),
            this::requiresRewrite,
            this::preSpanQueryRewrite,
            !handleMultiTermQuery
        )
        : PhraseHelper.NONE;
  }

  protected CharacterRunAutomaton[] getAutomata(String field, Query query, Set<HighlightFlag> highlightFlags) {
    // do we "eagerly" look in span queries for automata here, or do we not and let PhraseHelper handle those?
    // if don't highlight phrases strictly,
    final boolean lookInSpan =
        !highlightFlags.contains(HighlightFlag.PHRASES) // no PhraseHelper
        || highlightFlags.contains(HighlightFlag.WEIGHT_MATCHES); // Weight.Matches will find all

    return highlightFlags.contains(HighlightFlag.MULTI_TERM_QUERY)
        ? MultiTermHighlighting.extractAutomata(query, getFieldMatcher(field), lookInSpan)
        : ZERO_LEN_AUTOMATA_ARRAY;
  }

  protected OffsetSource getOptimizedOffsetSource(UHComponents components) {
    OffsetSource offsetSource = getOffsetSource(components.getField());

    // null automata means unknown, so assume a possibility
    boolean mtqOrRewrite = components.getAutomata() == null || components.getAutomata().length > 0
        || components.getPhraseHelper().willRewrite() || components.hasUnrecognizedQueryPart();

    // null terms means unknown, so assume something to highlight
    if (mtqOrRewrite == false && components.getTerms() != null && components.getTerms().length == 0) {
      return OffsetSource.NONE_NEEDED; //nothing to highlight
    }

    switch (offsetSource) {
      case POSTINGS:
        if (mtqOrRewrite) { // may need to see scan through all terms for the highlighted document efficiently
          return OffsetSource.ANALYSIS;
        }
        break;
      case POSTINGS_WITH_TERM_VECTORS:
        if (mtqOrRewrite == false) {
          return OffsetSource.POSTINGS; //We don't need term vectors
        }
        break;
      case ANALYSIS:
      case TERM_VECTORS:
      case NONE_NEEDED:
      default:
        //stick with the original offset source
        break;
    }

    return offsetSource;
  }

  protected FieldOffsetStrategy getOffsetStrategy(OffsetSource offsetSource, UHComponents components) {
    switch (offsetSource) {
      case ANALYSIS:
        if (!components.getPhraseHelper().hasPositionSensitivity() &&
            !components.getHighlightFlags().contains(HighlightFlag.PASSAGE_RELEVANCY_OVER_SPEED) &&
            !components.getHighlightFlags().contains(HighlightFlag.WEIGHT_MATCHES)) {
          //skip using a memory index since it's pure term filtering
          return new TokenStreamOffsetStrategy(components, getIndexAnalyzer());
        } else {
          return new MemoryIndexOffsetStrategy(components, getIndexAnalyzer());
        }
      case NONE_NEEDED:
        return NoOpOffsetStrategy.INSTANCE;
      case TERM_VECTORS:
        return new TermVectorOffsetStrategy(components);
      case POSTINGS:
        return new PostingsOffsetStrategy(components);
      case POSTINGS_WITH_TERM_VECTORS:
        return new PostingsWithTermVectorsOffsetStrategy(components);
      default:
        throw new IllegalArgumentException("Unrecognized offset source " + offsetSource);
    }
  }

  /**
   * When highlighting phrases accurately, we need to know which {@link SpanQuery}'s need to have
   * {@link Query#rewrite(IndexReader)} called on them.  It helps performance to avoid it if it's not needed.
   * This method will be invoked on all SpanQuery instances recursively. If you have custom SpanQuery queries then
   * override this to check instanceof and provide a definitive answer. If the query isn't your custom one, simply
   * return null to have the default rules apply, which govern the ones included in Lucene.
   */
  protected Boolean requiresRewrite(SpanQuery spanQuery) {
    return null;
  }

  /**
   * When highlighting phrases accurately, we may need to handle custom queries that aren't supported in the
   * {@link org.apache.lucene.search.highlight.WeightedSpanTermExtractor} as called by the {@code PhraseHelper}.
   * Should custom query types be needed, this method should be overriden to return a collection of queries if appropriate,
   * or null if nothing to do. If the query is not custom, simply returning null will allow the default rules to apply.
   *
   * @param query Query to be highlighted
   * @return A Collection of Query object(s) if needs to be rewritten, otherwise null.
   */
  protected Collection<Query> preSpanQueryRewrite(Query query) {
    return null;
  }

  private DocIdSetIterator asDocIdSetIterator(int[] sortedDocIds) {
    return new DocIdSetIterator() {
      int idx = -1;

      @Override
      public int docID() {
        if (idx < 0 || idx >= sortedDocIds.length) {
          return NO_MORE_DOCS;
        }
        return sortedDocIds[idx];
      }

      @Override
      public int nextDoc() throws IOException {
        idx++;
        return docID();
      }

      @Override
      public int advance(int target) throws IOException {
        return super.slowAdvance(target); // won't be called, so whatever
      }

      @Override
      public long cost() {
        return Math.max(0, sortedDocIds.length - (idx + 1)); // remaining docs
      }
    };
  }

  /**
   * Loads the String values for each docId by field to be highlighted.  By default this loads from stored fields
   * by the same name as given, but a subclass can change the source.  The returned Strings must be identical to
   * what was indexed (at least for postings or term-vectors offset sources).
   * This method must load fields for at least one document from the given {@link DocIdSetIterator}
   * but need not return all of them; by default the character lengths are summed and this method will return early
   * when {@code cacheCharsThreshold} is exceeded.  Specifically if that number is 0, then only one document is
   * fetched no matter what.  Values in the array of {@link CharSequence} will be null if no value was found.
   */
  protected List<CharSequence[]> loadFieldValues(String[] fields,
                                                 DocIdSetIterator docIter, int cacheCharsThreshold)
      throws IOException {
    List<CharSequence[]> docListOfFields =
        new ArrayList<>(cacheCharsThreshold == 0 ? 1 : (int) Math.min(64, docIter.cost()));

    LimitedStoredFieldVisitor visitor = newLimitedStoredFieldsVisitor(fields);
    int sumChars = 0;
    do {
      int docId = docIter.nextDoc();
      if (docId == DocIdSetIterator.NO_MORE_DOCS) {
        break;
      }
      visitor.init();
      searcher.doc(docId, visitor);
      CharSequence[] valuesByField = visitor.getValuesByField();
      docListOfFields.add(valuesByField);
      for (CharSequence val : valuesByField) {
        sumChars += (val == null ? 0 : val.length());
      }
    } while (sumChars <= cacheCharsThreshold && cacheCharsThreshold != 0);
    return docListOfFields;
  }

  /**
   * @lucene.internal
   */
  protected LimitedStoredFieldVisitor newLimitedStoredFieldsVisitor(String[] fields) {
    return new LimitedStoredFieldVisitor(fields, MULTIVAL_SEP_CHAR, getMaxLength());
  }

  /**
   * Fetches stored fields for highlighting. Uses a multi-val separator char and honors a max length to retrieve.
   * @lucene.internal
   */
  protected static class LimitedStoredFieldVisitor extends StoredFieldVisitor {
    protected final String[] fields;
    protected final char valueSeparator;
    protected final int maxLength;
    protected CharSequence[] values;// starts off as String; may become StringBuilder.
    protected int currentField;

    public LimitedStoredFieldVisitor(String[] fields, char valueSeparator, int maxLength) {
      this.fields = fields;
      this.valueSeparator = valueSeparator;
      this.maxLength = maxLength;
    }

    void init() {
      values = new CharSequence[fields.length];
      currentField = -1;
    }

    @Override
    public void stringField(FieldInfo fieldInfo, String value) throws IOException {
      assert currentField >= 0;
      Objects.requireNonNull(value, "String value should not be null");
      CharSequence curValue = values[currentField];
      if (curValue == null) {
        //question: if truncate due to maxLength, should we try and avoid keeping the other chars in-memory on
        //  the backing char[]?
        values[currentField] = value.substring(0, Math.min(maxLength, value.length()));//note: may return 'this'
        return;
      }
      final int lengthBudget = maxLength - curValue.length();
      if (lengthBudget <= 0) {
        return;
      }
      StringBuilder curValueBuilder;
      if (curValue instanceof StringBuilder) {
        curValueBuilder = (StringBuilder) curValue;
      } else {
        // upgrade String to StringBuilder. Choose a good initial size.
        curValueBuilder = new StringBuilder(curValue.length() + Math.min(lengthBudget, value.length() + 256));
        curValueBuilder.append(curValue);
      }
      curValueBuilder.append(valueSeparator);
      curValueBuilder.append(value.substring(0, Math.min(lengthBudget - 1, value.length())));
      values[currentField] = curValueBuilder;
    }

    @Override
    public Status needsField(FieldInfo fieldInfo) throws IOException {
      currentField = Arrays.binarySearch(fields, fieldInfo.name);
      if (currentField < 0) {
        return Status.NO;
      }
      CharSequence curVal = values[currentField];
      if (curVal != null && curVal.length() >= maxLength) {
        return fields.length == 1 ? Status.STOP : Status.NO;
      }
      return Status.YES;
    }

    CharSequence[] getValuesByField() {
      return this.values;
    }

  }

  /**
   * Wraps an IndexReader that remembers/caches the last call to {@link LeafReader#getTermVectors(int)} so that
   * if the next call has the same ID, then it is reused.  If TV's were column-stride (like doc-values), there would
   * be no need for this.
   */
  private static class TermVectorReusingLeafReader extends FilterLeafReader {

    static IndexReader wrap(IndexReader reader) throws IOException {
      LeafReader[] leafReaders = reader.leaves().stream()
          .map(LeafReaderContext::reader)
          .map(TermVectorReusingLeafReader::new)
          .toArray(LeafReader[]::new);
      return new BaseCompositeReader<IndexReader>(leafReaders) {
        @Override
        protected void doClose() throws IOException {
          reader.close();
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
          return null;
        }
      };
    }

    private int lastDocId = -1;
    private Fields tvFields;

    TermVectorReusingLeafReader(LeafReader in) {
      super(in);
    }

    @Override
    public Fields getTermVectors(int docID) throws IOException {
      if (docID != lastDocId) {
        lastDocId = docID;
        tvFields = in.getTermVectors(docID);
      }
      return tvFields;
    }

    @Override
    public CacheHelper getCoreCacheHelper() {
      return null;
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
      return null;
    }

  }

  /**
   * Flags for controlling highlighting behavior.
   */
  public enum HighlightFlag {
    /** @see UnifiedHighlighter#setHighlightPhrasesStrictly(boolean) */
    PHRASES,

    /** @see UnifiedHighlighter#setHandleMultiTermQuery(boolean) */
    MULTI_TERM_QUERY,

    /** Passage relevancy is more important than speed.  True by default. */
    PASSAGE_RELEVANCY_OVER_SPEED,

    /**
     * Internally use the {@link Weight#matches(LeafReaderContext, int)} API for highlighting.
     * It's more accurate to the query, though might not calculate passage relevancy as well.
     * Use of this flag requires {@link #MULTI_TERM_QUERY} and {@link #PHRASES}.
     * {@link #PASSAGE_RELEVANCY_OVER_SPEED} will be ignored.  False by default.
     */
    WEIGHT_MATCHES

    // TODO: useQueryBoosts
  }
}
