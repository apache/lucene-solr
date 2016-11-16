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
package org.apache.lucene.search.suggest.analyzing;

import java.io.Closeable;
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.AnalyzerWrapper;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.ngram.EdgeNGramTokenFilter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.EarlyTerminatingSortingCollector;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.suggest.InputIterator;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;

// TODO:
//   - a PostingsFormat that stores super-high-freq terms as
//     a bitset should be a win for the prefix terms?
//     (LUCENE-5052)
//   - we could offer a better integration with
//     DocumentDictionary and NRT?  so that your suggester
//     "automatically" keeps in sync w/ your index

/** Analyzes the input text and then suggests matches based
 *  on prefix matches to any tokens in the indexed text.
 *  This also highlights the tokens that match.
 *
 *  <p>This suggester supports payloads.  Matches are sorted only
 *  by the suggest weight; it would be nice to support
 *  blended score + weight sort in the future.  This means
 *  this suggester best applies when there is a strong
 *  a-priori ranking of all the suggestions.
 *
 *  <p>This suggester supports contexts, including arbitrary binary
 *  terms.
 *
 * @lucene.experimental */    

public class AnalyzingInfixSuggester extends Lookup implements Closeable {

  /** Field name used for the indexed text. */
  protected final static String TEXT_FIELD_NAME = "text";

  /** Field name used for the indexed text, as a
   *  StringField, for exact lookup. */
  protected final static String EXACT_TEXT_FIELD_NAME = "exacttext";

  /** Field name used for the indexed context, as a
   *  StringField and a SortedSetDVField, for filtering. */
  protected final static String CONTEXTS_FIELD_NAME = "contexts";

  /** Analyzer used at search time */
  protected final Analyzer queryAnalyzer;
  /** Analyzer used at index time */
  protected final Analyzer indexAnalyzer;
  private final Directory dir;
  final int minPrefixChars;
  
  private final boolean allTermsRequired;
  private final boolean highlight;
  
  private final boolean commitOnBuild;
  private final boolean closeIndexWriterOnBuild;

  /** Used for ongoing NRT additions/updates. */
  protected IndexWriter writer;

  /** {@link IndexSearcher} used for lookups. */
  protected SearcherManager searcherMgr;

  /** Default minimum number of leading characters before
   *  PrefixQuery is used (4). */
  public static final int DEFAULT_MIN_PREFIX_CHARS = 4;
  
  /** Default boolean clause option for multiple terms matching (all terms required). */
  public static final boolean DEFAULT_ALL_TERMS_REQUIRED = true;
 
  /** Default higlighting option. */
  public static final boolean DEFAULT_HIGHLIGHT = true;

  /** Default option to close the IndexWriter once the index has been built. */
  protected final static boolean DEFAULT_CLOSE_INDEXWRITER_ON_BUILD = true;

  /** How we sort the postings and search results. */
  private static final Sort SORT = new Sort(new SortField("weight", SortField.Type.LONG, true));

  /** Create a new instance, loading from a previously built
   *  AnalyzingInfixSuggester directory, if it exists.  This directory must be
   *  private to the infix suggester (i.e., not an external
   *  Lucene index).  Note that {@link #close}
   *  will also close the provided directory. */
  public AnalyzingInfixSuggester(Directory dir, Analyzer analyzer) throws IOException {
    this(dir, analyzer, analyzer, DEFAULT_MIN_PREFIX_CHARS, false, DEFAULT_ALL_TERMS_REQUIRED, DEFAULT_HIGHLIGHT);
  }
  
  /** Create a new instance, loading from a previously built
   *  AnalyzingInfixSuggester directory, if it exists.  This directory must be
   *  private to the infix suggester (i.e., not an external
   *  Lucene index).  Note that {@link #close}
   *  will also close the provided directory.
   *
   *  @param minPrefixChars Minimum number of leading characters
   *     before PrefixQuery is used (default 4).
   *     Prefixes shorter than this are indexed as character
   *     ngrams (increasing index size but making lookups
   *     faster).
   *
   *  @param commitOnBuild Call commit after the index has finished building. This would persist the
   *                       suggester index to disk and future instances of this suggester can use this pre-built dictionary.
   */
  public AnalyzingInfixSuggester(Directory dir, Analyzer indexAnalyzer, Analyzer queryAnalyzer, int minPrefixChars,
                                 boolean commitOnBuild) throws IOException {
    this(dir, indexAnalyzer, queryAnalyzer, minPrefixChars, commitOnBuild, DEFAULT_ALL_TERMS_REQUIRED, DEFAULT_HIGHLIGHT);
  }
  
  /** Create a new instance, loading from a previously built
   *  AnalyzingInfixSuggester directory, if it exists.  This directory must be
   *  private to the infix suggester (i.e., not an external
   *  Lucene index).  Note that {@link #close}
   *  will also close the provided directory.
   *
   *  @param minPrefixChars Minimum number of leading characters
   *     before PrefixQuery is used (default 4).
   *     Prefixes shorter than this are indexed as character
   *     ngrams (increasing index size but making lookups
   *     faster).
   *
   *  @param commitOnBuild Call commit after the index has finished building. This would persist the
   *                       suggester index to disk and future instances of this suggester can use this pre-built dictionary.
   *
   *  @param allTermsRequired All terms in the suggest query must be matched.
   *  @param highlight Highlight suggest query in suggestions.
   *
   */
  public AnalyzingInfixSuggester(Directory dir, Analyzer indexAnalyzer, Analyzer queryAnalyzer, int minPrefixChars,
                                 boolean commitOnBuild,
                                 boolean allTermsRequired, boolean highlight) throws IOException {
    this(dir, indexAnalyzer, queryAnalyzer, minPrefixChars, commitOnBuild, allTermsRequired, highlight, 
         DEFAULT_CLOSE_INDEXWRITER_ON_BUILD);
  }

    /** Create a new instance, loading from a previously built
     *  AnalyzingInfixSuggester directory, if it exists.  This directory must be
     *  private to the infix suggester (i.e., not an external
     *  Lucene index).  Note that {@link #close}
     *  will also close the provided directory.
     *
     *  @param minPrefixChars Minimum number of leading characters
     *     before PrefixQuery is used (default 4).
     *     Prefixes shorter than this are indexed as character
     *     ngrams (increasing index size but making lookups
     *     faster).
     *
     *  @param commitOnBuild Call commit after the index has finished building. This would persist the
     *                       suggester index to disk and future instances of this suggester can use this pre-built dictionary.
     *
     *  @param allTermsRequired All terms in the suggest query must be matched.
     *  @param highlight Highlight suggest query in suggestions.
     *  @param closeIndexWriterOnBuild If true, the IndexWriter will be closed after the index has finished building.
     */
  public AnalyzingInfixSuggester(Directory dir, Analyzer indexAnalyzer, Analyzer queryAnalyzer, int minPrefixChars,
                                 boolean commitOnBuild, boolean allTermsRequired, 
                                 boolean highlight, boolean closeIndexWriterOnBuild) throws IOException {
                                    
    if (minPrefixChars < 0) {
      throw new IllegalArgumentException("minPrefixChars must be >= 0; got: " + minPrefixChars);
    }

    this.queryAnalyzer = queryAnalyzer;
    this.indexAnalyzer = indexAnalyzer;
    this.dir = dir;
    this.minPrefixChars = minPrefixChars;
    this.commitOnBuild = commitOnBuild;
    this.allTermsRequired = allTermsRequired;
    this.highlight = highlight;
    this.closeIndexWriterOnBuild = closeIndexWriterOnBuild;

    if (DirectoryReader.indexExists(dir)) {
      // Already built; open it:
      writer = new IndexWriter(dir,
                               getIndexWriterConfig(getGramAnalyzer(), IndexWriterConfig.OpenMode.APPEND));
      searcherMgr = new SearcherManager(writer, null);
    }
  }

  /** Override this to customize index settings, e.g. which
   *  codec to use. */
  protected IndexWriterConfig getIndexWriterConfig(Analyzer indexAnalyzer, IndexWriterConfig.OpenMode openMode) {
    IndexWriterConfig iwc = new IndexWriterConfig(indexAnalyzer);
    iwc.setOpenMode(openMode);

    // This way all merged segments will be sorted at
    // merge time, allow for per-segment early termination
    // when those segments are searched:
    iwc.setIndexSort(SORT);

    return iwc;
  }

  /** Subclass can override to choose a specific {@link
   *  Directory} implementation. */
  protected Directory getDirectory(Path path) throws IOException {
    return FSDirectory.open(path);
  }

  @Override
  public void build(InputIterator iter) throws IOException {
    
    if (searcherMgr != null) {
      searcherMgr.close();
      searcherMgr = null;
    }

    if (writer != null) {
      writer.close();
      writer = null;
    }

    boolean success = false;
    try {
      // First pass: build a temporary normal Lucene index,
      // just indexing the suggestions as they iterate:
      writer = new IndexWriter(dir,
                               getIndexWriterConfig(getGramAnalyzer(), IndexWriterConfig.OpenMode.CREATE));
      //long t0 = System.nanoTime();

      // TODO: use threads?
      BytesRef text;
      while ((text = iter.next()) != null) {
        BytesRef payload;
        if (iter.hasPayloads()) {
          payload = iter.payload();
        } else {
          payload = null;
        }

        add(text, iter.contexts(), iter.weight(), payload);
      }

      //System.out.println("initial indexing time: " + ((System.nanoTime()-t0)/1000000) + " msec");
      if (commitOnBuild || closeIndexWriterOnBuild) {
        commit();
      }
      searcherMgr = new SearcherManager(writer, null);
      success = true;
    } finally {
      if (success) {
        if (closeIndexWriterOnBuild) {
          writer.close();
          writer = null;
        }
      } else {  // failure
        if (writer != null) {
          writer.rollback();
          writer = null;
        }
      }
    }
  }

  /** Commits all pending changes made to this suggester to disk.
   *
   *  @see IndexWriter#commit */
  public void commit() throws IOException {
    if (writer == null) {
      if (searcherMgr == null || closeIndexWriterOnBuild == false) {
        throw new IllegalStateException("Cannot commit on an closed writer. Add documents first");
      }
      // else no-op: writer was committed and closed after the index was built, so commit is unnecessary
    } else {
      writer.commit();
    }
  }

  private Analyzer getGramAnalyzer() {
    return new AnalyzerWrapper(Analyzer.PER_FIELD_REUSE_STRATEGY) {
      @Override
      protected Analyzer getWrappedAnalyzer(String fieldName) {
        return indexAnalyzer;
      }

      @Override
      protected TokenStreamComponents wrapComponents(String fieldName, TokenStreamComponents components) {
        if (fieldName.equals("textgrams") && minPrefixChars > 0) {
          // TODO: should use an EdgeNGramTokenFilterFactory here
          TokenFilter filter = new EdgeNGramTokenFilter(components.getTokenStream(), 1, minPrefixChars);
          return new TokenStreamComponents(components.getTokenizer(), filter);
        } else {
          return components;
        }
      }
    };
  }

  private synchronized void ensureOpen() throws IOException {
    if (writer == null) {
      if (DirectoryReader.indexExists(dir)) {
        // Already built; open it:
        writer = new IndexWriter(dir, getIndexWriterConfig(getGramAnalyzer(), IndexWriterConfig.OpenMode.APPEND));
      } else {
        writer = new IndexWriter(dir, getIndexWriterConfig(getGramAnalyzer(), IndexWriterConfig.OpenMode.CREATE));
      }
      SearcherManager oldSearcherMgr = searcherMgr;
      searcherMgr = new SearcherManager(writer, null);
      if (oldSearcherMgr != null) {
        oldSearcherMgr.close();
      }
    }
  }

  /** Adds a new suggestion.  Be sure to use {@link #update}
   *  instead if you want to replace a previous suggestion.
   *  After adding or updating a batch of new suggestions,
   *  you must call {@link #refresh} in the end in order to
   *  see the suggestions in {@link #lookup} */
  public void add(BytesRef text, Set<BytesRef> contexts, long weight, BytesRef payload) throws IOException {
    ensureOpen();
    writer.addDocument(buildDocument(text, contexts, weight, payload));
  }

  /** Updates a previous suggestion, matching the exact same
   *  text as before.  Use this to change the weight or
   *  payload of an already added suggestion.  If you know
   *  this text is not already present you can use {@link
   *  #add} instead.  After adding or updating a batch of
   *  new suggestions, you must call {@link #refresh} in the
   *  end in order to see the suggestions in {@link #lookup} */
  public void update(BytesRef text, Set<BytesRef> contexts, long weight, BytesRef payload) throws IOException {
    ensureOpen();
    writer.updateDocument(new Term(EXACT_TEXT_FIELD_NAME, text.utf8ToString()),
                          buildDocument(text, contexts, weight, payload));
  }

  private Document buildDocument(BytesRef text, Set<BytesRef> contexts, long weight, BytesRef payload) throws IOException {
    String textString = text.utf8ToString();
    Document doc = new Document();
    FieldType ft = getTextFieldType();
    doc.add(new Field(TEXT_FIELD_NAME, textString, ft));
    doc.add(new Field("textgrams", textString, ft));
    doc.add(new StringField(EXACT_TEXT_FIELD_NAME, textString, Field.Store.NO));
    doc.add(new BinaryDocValuesField(TEXT_FIELD_NAME, text));
    doc.add(new NumericDocValuesField("weight", weight));
    if (payload != null) {
      doc.add(new BinaryDocValuesField("payloads", payload));
    }
    if (contexts != null) {
      for(BytesRef context : contexts) {
        doc.add(new StringField(CONTEXTS_FIELD_NAME, context, Field.Store.NO));
        doc.add(new SortedSetDocValuesField(CONTEXTS_FIELD_NAME, context));
      }
    }
    return doc;
  }

  /** Reopens the underlying searcher; it's best to "batch
   *  up" many additions/updates, and then call refresh
   *  once in the end. */
  public void refresh() throws IOException {
    if (searcherMgr == null) {
      throw new IllegalStateException("suggester was not built");
    }
    if (writer != null) {
      searcherMgr.maybeRefreshBlocking();
    }
    // else no-op: writer was committed and closed after the index was built
    //             and before searchMgr was constructed, so refresh is unnecessary
  }

  /**
   * Subclass can override this method to change the field type of the text field
   * e.g. to change the index options
   */
  protected FieldType getTextFieldType(){
    FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
    ft.setIndexOptions(IndexOptions.DOCS);
    ft.setOmitNorms(true);

    return ft;
  }

  @Override
  public List<LookupResult> lookup(CharSequence key, Set<BytesRef> contexts, boolean onlyMorePopular, int num) throws IOException {
    return lookup(key, contexts, num, allTermsRequired, highlight);
  }

  /** Lookup, without any context. */
  public List<LookupResult> lookup(CharSequence key, int num, boolean allTermsRequired, boolean doHighlight) throws IOException {
    return lookup(key, (BooleanQuery)null, num, allTermsRequired, doHighlight);
  }

  /** Lookup, with context but without booleans. Context booleans default to SHOULD,
   *  so each suggestion must have at least one of the contexts. */
  public List<LookupResult> lookup(CharSequence key, Set<BytesRef> contexts, int num, boolean allTermsRequired, boolean doHighlight) throws IOException {
    return lookup(key, toQuery(contexts), num, allTermsRequired, doHighlight);
  }

  /** This is called if the last token isn't ended
   *  (e.g. user did not type a space after it).  Return an
   *  appropriate Query clause to add to the BooleanQuery. */
  protected Query getLastTokenQuery(String token) throws IOException {
    if (token.length() < minPrefixChars) {
      // The leading ngram was directly indexed:
      return new TermQuery(new Term("textgrams", token));
    }

    return new PrefixQuery(new Term(TEXT_FIELD_NAME, token));
  }

  /** Retrieve suggestions, specifying whether all terms
   *  must match ({@code allTermsRequired}) and whether the hits
   *  should be highlighted ({@code doHighlight}). */
  public List<LookupResult> lookup(CharSequence key, Map<BytesRef, BooleanClause.Occur> contextInfo, int num, boolean allTermsRequired, boolean doHighlight) throws IOException {
      return lookup(key, toQuery(contextInfo), num, allTermsRequired, doHighlight);
  }

  private BooleanQuery toQuery(Map<BytesRef,BooleanClause.Occur> contextInfo) {
    if (contextInfo == null || contextInfo.isEmpty()) {
      return null;
    }
    
    BooleanQuery.Builder contextFilter = new BooleanQuery.Builder();
    for (Map.Entry<BytesRef,BooleanClause.Occur> entry : contextInfo.entrySet()) {
      addContextToQuery(contextFilter, entry.getKey(), entry.getValue());
    }
    
    return contextFilter.build();
  }

  private BooleanQuery toQuery(Set<BytesRef> contextInfo) {
    if (contextInfo == null || contextInfo.isEmpty()) {
      return null;
    }
    
    BooleanQuery.Builder contextFilter = new BooleanQuery.Builder();
    for (BytesRef context : contextInfo) {
      addContextToQuery(contextFilter, context, BooleanClause.Occur.SHOULD);
    }
    return contextFilter.build();
  }

  
  /**
   * This method is handy as we do not need access to internal fields such as CONTEXTS_FIELD_NAME in order to build queries
   * However, here may not be its best location.
   * 
   * @param query an instance of @See {@link BooleanQuery}
   * @param context the context
   * @param clause one of {@link Occur}
   */
  public void addContextToQuery(BooleanQuery.Builder query, BytesRef context, BooleanClause.Occur clause) {
    // NOTE: we "should" wrap this in
    // ConstantScoreQuery, or maybe send this as a
    // Filter instead to search.
    
    // TODO: if we had a BinaryTermField we could fix
    // this "must be valid ut8f" limitation:
    query.add(new TermQuery(new Term(CONTEXTS_FIELD_NAME, context)), clause);
  }

  /**
   * This is an advanced method providing the capability to send down to the suggester any 
   * arbitrary lucene query to be used to filter the result of the suggester
   * 
   * @param key the keyword being looked for
   * @param contextQuery an arbitrary Lucene query to be used to filter the result of the suggester. {@link #addContextToQuery} could be used to build this contextQuery.
   * @param num number of items to return
   * @param allTermsRequired all searched terms must match or not
   * @param doHighlight if true, the matching term will be highlighted in the search result
   * @return the result of the suggester
   * @throws IOException f the is IO exception while reading data from the index
   */
  public List<LookupResult> lookup(CharSequence key, BooleanQuery contextQuery, int num, boolean allTermsRequired, boolean doHighlight) throws IOException {

    if (searcherMgr == null) {
      throw new IllegalStateException("suggester was not built");
    }

    final BooleanClause.Occur occur;
    if (allTermsRequired) {
      occur = BooleanClause.Occur.MUST;
    } else {
      occur = BooleanClause.Occur.SHOULD;
    }

    BooleanQuery.Builder query;
    Set<String> matchedTokens;
    String prefixToken = null;

    try (TokenStream ts = queryAnalyzer.tokenStream("", new StringReader(key.toString()))) {
      //long t0 = System.currentTimeMillis();
      ts.reset();
      final CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
      final OffsetAttribute offsetAtt = ts.addAttribute(OffsetAttribute.class);
      String lastToken = null;
      query = new BooleanQuery.Builder();
      int maxEndOffset = -1;
      matchedTokens = new HashSet<>();
      while (ts.incrementToken()) {
        if (lastToken != null) {  
          matchedTokens.add(lastToken);
          query.add(new TermQuery(new Term(TEXT_FIELD_NAME, lastToken)), occur);
        }
        lastToken = termAtt.toString();
        if (lastToken != null) {
          maxEndOffset = Math.max(maxEndOffset, offsetAtt.endOffset());
        }
      }
      ts.end();

      if (lastToken != null) {
        Query lastQuery;
        if (maxEndOffset == offsetAtt.endOffset()) {
          // Use PrefixQuery (or the ngram equivalent) when
          // there was no trailing discarded chars in the
          // string (e.g. whitespace), so that if query does
          // not end with a space we show prefix matches for
          // that token:
          lastQuery = getLastTokenQuery(lastToken);
          prefixToken = lastToken;
        } else {
          // Use TermQuery for an exact match if there were
          // trailing discarded chars (e.g. whitespace), so
          // that if query ends with a space we only show
          // exact matches for that term:
          matchedTokens.add(lastToken);
          lastQuery = new TermQuery(new Term(TEXT_FIELD_NAME, lastToken));
        }
        
        if (lastQuery != null) {
          query.add(lastQuery, occur);
        }
      }

      if (contextQuery != null) {
        boolean allMustNot = true;
        for (BooleanClause clause : contextQuery.clauses()) {
          if (clause.getOccur() != BooleanClause.Occur.MUST_NOT) {
            allMustNot = false;
            break;
          }
        }
        
        if (allMustNot) {
          // All are MUST_NOT: add the contextQuery to the main query instead (not as sub-query)
          for (BooleanClause clause : contextQuery.clauses()) {
            query.add(clause);
          }
        } else if (allTermsRequired == false) {
          // We must carefully upgrade the query clauses to MUST:
          BooleanQuery.Builder newQuery = new BooleanQuery.Builder();
          newQuery.add(query.build(), BooleanClause.Occur.MUST);
          newQuery.add(contextQuery, BooleanClause.Occur.MUST);
          query = newQuery;
        } else {
          // Add contextQuery as sub-query
          query.add(contextQuery, BooleanClause.Occur.MUST);
        }
      }
    }
    
    // TODO: we could allow blended sort here, combining
    // weight w/ score.  Now we ignore score and sort only
    // by weight:

    Query finalQuery = finishQuery(query, allTermsRequired);

    //System.out.println("finalQuery=" + finalQuery);

    // Sort by weight, descending:
    TopFieldCollector c = TopFieldCollector.create(SORT, num, true, false, false);

    // We sorted postings by weight during indexing, so we
    // only retrieve the first num hits now:
    Collector c2 = new EarlyTerminatingSortingCollector(c, SORT, num);
    List<LookupResult> results = null;
    IndexSearcher searcher = searcherMgr.acquire();
    try {
      //System.out.println("got searcher=" + searcher);
      searcher.search(finalQuery, c2);

      TopFieldDocs hits = c.topDocs();

      // Slower way if postings are not pre-sorted by weight:
      // hits = searcher.search(query, null, num, SORT);
      results = createResults(searcher, hits, num, key, doHighlight, matchedTokens, prefixToken);
    } finally {
      searcherMgr.release(searcher);
    }

    //System.out.println((System.currentTimeMillis() - t0) + " msec for infix suggest");
    //System.out.println(results);

    return results;
  }
  
  /**
   * Create the results based on the search hits.
   * Can be overridden by subclass to add particular behavior (e.g. weight transformation).
   * Note that there is no prefix toke (the {@code prefixToken} argument will
   * be null) whenever the final token in the incoming request was in fact finished
   * (had trailing characters, such as white-space).
   *
   * @throws IOException If there are problems reading fields from the underlying Lucene index.
   */
  protected List<LookupResult> createResults(IndexSearcher searcher, TopFieldDocs hits, int num,
                                             CharSequence charSequence,
                                             boolean doHighlight, Set<String> matchedTokens, String prefixToken)
      throws IOException {

    List<LeafReaderContext> leaves = searcher.getIndexReader().leaves();
    List<LookupResult> results = new ArrayList<>();
    for (int i=0;i<hits.scoreDocs.length;i++) {
      FieldDoc fd = (FieldDoc) hits.scoreDocs[i];
      BinaryDocValues textDV = MultiDocValues.getBinaryValues(searcher.getIndexReader(), TEXT_FIELD_NAME);
      textDV.advance(fd.doc);
      BytesRef term = textDV.binaryValue();
      String text = term.utf8ToString();
      long score = (Long) fd.fields[0];

      // This will just be null if app didn't pass payloads to build():
      // TODO: maybe just stored fields?  they compress...
      BinaryDocValues payloadsDV = MultiDocValues.getBinaryValues(searcher.getIndexReader(), "payloads");

      BytesRef payload;
      if (payloadsDV != null) {
        if (payloadsDV.advance(fd.doc) == fd.doc) {
          payload = BytesRef.deepCopyOf(payloadsDV.binaryValue());
        } else {
          payload = new BytesRef(BytesRef.EMPTY_BYTES);
        }
      } else {
        payload = null;
      }

      // Must look up sorted-set by segment:
      int segment = ReaderUtil.subIndex(fd.doc, leaves);
      SortedSetDocValues contextsDV = leaves.get(segment).reader().getSortedSetDocValues(CONTEXTS_FIELD_NAME);
      Set<BytesRef> contexts;
      if (contextsDV != null) {
        contexts = new HashSet<BytesRef>();
        int targetDocID = fd.doc - leaves.get(segment).docBase;
        if (contextsDV.advance(targetDocID) == targetDocID) {
          long ord;
          while ((ord = contextsDV.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
            BytesRef context = BytesRef.deepCopyOf(contextsDV.lookupOrd(ord));
            contexts.add(context);
          }
        }
      } else {
        contexts = null;
      }

      LookupResult result;

      if (doHighlight) {
        result = new LookupResult(text, highlight(text, matchedTokens, prefixToken), score, payload, contexts);
      } else {
        result = new LookupResult(text, score, payload, contexts);
      }

      results.add(result);
    }

    return results;
  }

  /** Subclass can override this to tweak the Query before
   *  searching. */
  protected Query finishQuery(BooleanQuery.Builder in, boolean allTermsRequired) {
    return in.build();
  }

  /** Override this method to customize the Object
   *  representing a single highlighted suggestions; the
   *  result is set on each {@link
   *  org.apache.lucene.search.suggest.Lookup.LookupResult#highlightKey} member. */
  protected Object highlight(String text, Set<String> matchedTokens, String prefixToken) throws IOException {
    try (TokenStream ts = queryAnalyzer.tokenStream("text", new StringReader(text))) {
      CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
      OffsetAttribute offsetAtt = ts.addAttribute(OffsetAttribute.class);
      ts.reset();
      StringBuilder sb = new StringBuilder();
      int upto = 0;
      while (ts.incrementToken()) {
        String token = termAtt.toString();
        int startOffset = offsetAtt.startOffset();
        int endOffset = offsetAtt.endOffset();
        if (upto < startOffset) {
          addNonMatch(sb, text.substring(upto, startOffset));
          upto = startOffset;
        } else if (upto > startOffset) {
          continue;
        }
        
        if (matchedTokens.contains(token)) {
          // Token matches.
          addWholeMatch(sb, text.substring(startOffset, endOffset), token);
          upto = endOffset;
        } else if (prefixToken != null && token.startsWith(prefixToken)) {
          addPrefixMatch(sb, text.substring(startOffset, endOffset), token, prefixToken);
          upto = endOffset;
        }
      }
      ts.end();
      int endOffset = offsetAtt.endOffset();
      if (upto < endOffset) {
        addNonMatch(sb, text.substring(upto));
      }
      return sb.toString();
    }
  }

  /** Called while highlighting a single result, to append a
   *  non-matching chunk of text from the suggestion to the
   *  provided fragments list.
   *  @param sb The {@code StringBuilder} to append to
   *  @param text The text chunk to add
   */
  protected void addNonMatch(StringBuilder sb, String text) {
    sb.append(text);
  }

  /** Called while highlighting a single result, to append
   *  the whole matched token to the provided fragments list.
   *  @param sb The {@code StringBuilder} to append to
   *  @param surface The surface form (original) text
   *  @param analyzed The analyzed token corresponding to the surface form text
   */
  protected void addWholeMatch(StringBuilder sb, String surface, String analyzed) {
    sb.append("<b>");
    sb.append(surface);
    sb.append("</b>");
  }

  /** Called while highlighting a single result, to append a
   *  matched prefix token, to the provided fragments list.
   *  @param sb The {@code StringBuilder} to append to
   *  @param surface The fragment of the surface form
   *        (indexed during {@link #build}, corresponding to
   *        this match
   *  @param analyzed The analyzed token that matched
   *  @param prefixToken The prefix of the token that matched
   */
  protected void addPrefixMatch(StringBuilder sb, String surface, String analyzed, String prefixToken) {
    // TODO: apps can try to invert their analysis logic
    // here, e.g. downcase the two before checking prefix:
    if (prefixToken.length() >= surface.length()) {
      addWholeMatch(sb, surface, analyzed);
      return;
    }
    sb.append("<b>");
    sb.append(surface.substring(0, prefixToken.length()));
    sb.append("</b>");
    sb.append(surface.substring(prefixToken.length()));
  }

  @Override
  public boolean store(DataOutput in) throws IOException {
    return false;
  }

  @Override
  public boolean load(DataInput out) throws IOException {
    return false;
  }

  @Override
  public void close() throws IOException {
    if (searcherMgr != null) {
      searcherMgr.close();
      searcherMgr = null;
    }
    if (writer != null) {
      writer.close();
      writer = null;
    }
    if (dir != null) {
      dir.close();
    }
  }

  @Override
  public long ramBytesUsed() {
    long mem = RamUsageEstimator.shallowSizeOf(this);
    try {
      if (searcherMgr != null) {
        IndexSearcher searcher = searcherMgr.acquire();
        try {
          for (LeafReaderContext context : searcher.getIndexReader().leaves()) {
            LeafReader reader = FilterLeafReader.unwrap(context.reader());
            if (reader instanceof SegmentReader) {
              mem += ((SegmentReader) context.reader()).ramBytesUsed();
            }
          }
        } finally {
          searcherMgr.release(searcher);
        }
      }
      return mem;
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public Collection<Accountable> getChildResources() {
    List<Accountable> resources = new ArrayList<>();
    try {
      if (searcherMgr != null) {
        IndexSearcher searcher = searcherMgr.acquire();
        try {
          for (LeafReaderContext context : searcher.getIndexReader().leaves()) {
            LeafReader reader = FilterLeafReader.unwrap(context.reader());
            if (reader instanceof SegmentReader) {
              resources.add(Accountables.namedAccountable("segment", (SegmentReader)reader));
            }
          }
        } finally {
          searcherMgr.release(searcher);
        }
      }
      return Collections.unmodifiableList(resources);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public long getCount() throws IOException {
    if (searcherMgr == null) {
      return 0;
    }
    IndexSearcher searcher = searcherMgr.acquire();
    try {
      return searcher.getIndexReader().numDocs();
    } finally {
      searcherMgr.release(searcher);
    }
  }
};
