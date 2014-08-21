package org.apache.lucene.search.suggest.analyzing;

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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.AnalyzerWrapper;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.ngram.EdgeNGramTokenFilter;
import org.apache.lucene.analysis.ngram.Lucene43EdgeNGramTokenFilter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.codecs.lucene410.Lucene410Codec;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.FilterAtomicReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.sorter.EarlyTerminatingSortingCollector;
import org.apache.lucene.index.sorter.SortingMergePolicy;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Collector;
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
import org.apache.lucene.search.suggest.Lookup.LookupResult; // javadocs
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.Version;

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
 *  <p>This suggester supports contexts, however the
 *  contexts must be valid utf8 (arbitrary binary terms will
 *  not work).
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
  final Version matchVersion;
  private final Directory dir;
  final int minPrefixChars;

  /** Used for ongoing NRT additions/updates. */
  private IndexWriter writer;

  /** {@link IndexSearcher} used for lookups. */
  protected SearcherManager searcherMgr;

  /** Default minimum number of leading characters before
   *  PrefixQuery is used (4). */
  public static final int DEFAULT_MIN_PREFIX_CHARS = 4;

  /** How we sort the postings and search results. */
  private static final Sort SORT = new Sort(new SortField("weight", SortField.Type.LONG, true));

  /** Create a new instance, loading from a previously built
   *  AnalyzingInfixSuggester directory, if it exists.  This directory must be
   *  private to the infix suggester (i.e., not an external
   *  Lucene index).  Note that {@link #close}
   *  will also close the provided directory. */
  public AnalyzingInfixSuggester(Version matchVersion, Directory dir, Analyzer analyzer) throws IOException {
    this(matchVersion, dir, analyzer, analyzer, DEFAULT_MIN_PREFIX_CHARS);
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
   */
  public AnalyzingInfixSuggester(Version matchVersion, Directory dir, Analyzer indexAnalyzer, Analyzer queryAnalyzer, int minPrefixChars) throws IOException {

    if (minPrefixChars < 0) {
      throw new IllegalArgumentException("minPrefixChars must be >= 0; got: " + minPrefixChars);
    }

    this.queryAnalyzer = queryAnalyzer;
    this.indexAnalyzer = indexAnalyzer;
    this.matchVersion = matchVersion;
    this.dir = dir;
    this.minPrefixChars = minPrefixChars;

    if (DirectoryReader.indexExists(dir)) {
      // Already built; open it:
      writer = new IndexWriter(dir,
                               getIndexWriterConfig(matchVersion, getGramAnalyzer(), IndexWriterConfig.OpenMode.APPEND));
      searcherMgr = new SearcherManager(writer, true, null);
    }
  }

  /** Override this to customize index settings, e.g. which
   *  codec to use. */
  protected IndexWriterConfig getIndexWriterConfig(Version matchVersion, Analyzer indexAnalyzer, IndexWriterConfig.OpenMode openMode) {
    IndexWriterConfig iwc = new IndexWriterConfig(matchVersion, indexAnalyzer);
    iwc.setCodec(new Lucene410Codec());
    iwc.setOpenMode(openMode);

    // This way all merged segments will be sorted at
    // merge time, allow for per-segment early termination
    // when those segments are searched:
    iwc.setMergePolicy(new SortingMergePolicy(iwc.getMergePolicy(), SORT));

    return iwc;
  }

  /** Subclass can override to choose a specific {@link
   *  Directory} implementation. */
  protected Directory getDirectory(File path) throws IOException {
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

    AtomicReader r = null;
    boolean success = false;
    try {
      // First pass: build a temporary normal Lucene index,
      // just indexing the suggestions as they iterate:
      writer = new IndexWriter(dir,
                               getIndexWriterConfig(matchVersion, getGramAnalyzer(), IndexWriterConfig.OpenMode.CREATE));
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

      searcherMgr = new SearcherManager(writer, true, null);
      success = true;
    } finally {
      if (success) {
        IOUtils.close(r);
      } else {
        IOUtils.closeWhileHandlingException(writer, r);
        writer = null;
      }
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

          return new TokenStreamComponents(components.getTokenizer(),
                                           new EdgeNGramTokenFilter(matchVersion,
                                                                    components.getTokenStream(),
                                                                    1, minPrefixChars));
        } else {
          return components;
        }
      }
    };
  }

  /** Adds a new suggestion.  Be sure to use {@link #update}
   *  instead if you want to replace a previous suggestion.
   *  After adding or updating a batch of new suggestions,
   *  you must call {@link #refresh} in the end in order to
   *  see the suggestions in {@link #lookup} */
  public void add(BytesRef text, Set<BytesRef> contexts, long weight, BytesRef payload) throws IOException {
    writer.addDocument(buildDocument(text, contexts, weight, payload));
  }

  /** Updates a previous suggestion, matching the exact same
   *  text as before.  Use this to change the weight or
   *  payload of an already added suggstion.  If you know
   *  this text is not already present you can use {@link
   *  #add} instead.  After adding or updating a batch of
   *  new suggestions, you must call {@link #refresh} in the
   *  end in order to see the suggestions in {@link #lookup} */
  public void update(BytesRef text, Set<BytesRef> contexts, long weight, BytesRef payload) throws IOException {
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
        // TODO: if we had a BinaryTermField we could fix
        // this "must be valid ut8f" limitation:
        doc.add(new StringField(CONTEXTS_FIELD_NAME, context.utf8ToString(), Field.Store.NO));
        doc.add(new SortedSetDocValuesField(CONTEXTS_FIELD_NAME, context));
      }
    }
    return doc;
  }

  /** Reopens the underlying searcher; it's best to "batch
   *  up" many additions/updates, and then call refresh
   *  once in the end. */
  public void refresh() throws IOException {
    searcherMgr.maybeRefreshBlocking();
  }

  /**
   * Subclass can override this method to change the field type of the text field
   * e.g. to change the index options
   */
  protected FieldType getTextFieldType(){
    FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
    ft.setIndexOptions(IndexOptions.DOCS_ONLY);
    ft.setOmitNorms(true);

    return ft;
  }

  @Override
  public List<LookupResult> lookup(CharSequence key, Set<BytesRef> contexts, boolean onlyMorePopular, int num) throws IOException {
    return lookup(key, contexts, num, true, true);
  }

  /** Lookup, without any context. */
  public List<LookupResult> lookup(CharSequence key, int num, boolean allTermsRequired, boolean doHighlight) throws IOException {
    return lookup(key, null, num, allTermsRequired, doHighlight);
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
  public List<LookupResult> lookup(CharSequence key, Set<BytesRef> contexts, int num, boolean allTermsRequired, boolean doHighlight) throws IOException {

    if (searcherMgr == null) {
      throw new IllegalStateException("suggester was not built");
    }

    final BooleanClause.Occur occur;
    if (allTermsRequired) {
      occur = BooleanClause.Occur.MUST;
    } else {
      occur = BooleanClause.Occur.SHOULD;
    }

    TokenStream ts = null;
    BooleanQuery query;
    Set<String> matchedTokens = new HashSet<>();
    String prefixToken = null;

    try {
      ts = queryAnalyzer.tokenStream("", new StringReader(key.toString()));

      //long t0 = System.currentTimeMillis();
      ts.reset();
      final CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
      final OffsetAttribute offsetAtt = ts.addAttribute(OffsetAttribute.class);
      String lastToken = null;
      query = new BooleanQuery();
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

      if (contexts != null) {
        BooleanQuery sub = new BooleanQuery();
        query.add(sub, BooleanClause.Occur.MUST);
        for(BytesRef context : contexts) {
          // NOTE: we "should" wrap this in
          // ConstantScoreQuery, or maybe send this as a
          // Filter instead to search, but since all of
          // these are MUST'd, the change to the score won't
          // affect the overall ranking.  Since we indexed
          // as DOCS_ONLY, the perf should be the same
          // either way (no freq int[] blocks to decode):

          // TODO: if we had a BinaryTermField we could fix
          // this "must be valid ut8f" limitation:
          sub.add(new TermQuery(new Term(CONTEXTS_FIELD_NAME, context.utf8ToString())), BooleanClause.Occur.SHOULD);
        }
      }
    } finally {
      IOUtils.closeWhileHandlingException(ts);
    }

    // TODO: we could allow blended sort here, combining
    // weight w/ score.  Now we ignore score and sort only
    // by weight:

    Query finalQuery = finishQuery(query, allTermsRequired);

    //System.out.println("finalQuery=" + query);

    // Sort by weight, descending:
    TopFieldCollector c = TopFieldCollector.create(SORT, num, true, false, false, false);

    // We sorted postings by weight during indexing, so we
    // only retrieve the first num hits now:
    Collector c2 = new EarlyTerminatingSortingCollector(c, SORT, num);
    IndexSearcher searcher = searcherMgr.acquire();
    List<LookupResult> results = null;
    try {
      //System.out.println("got searcher=" + searcher);
      searcher.search(finalQuery, c2);

      TopFieldDocs hits = (TopFieldDocs) c.topDocs();

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
   * Can be overridden by subclass to add particular behavior (e.g. weight transformation)
   * @throws IOException If there are problems reading fields from the underlying Lucene index.
   */
  protected List<LookupResult> createResults(IndexSearcher searcher, TopFieldDocs hits, int num,
                                             CharSequence charSequence,
                                             boolean doHighlight, Set<String> matchedTokens, String prefixToken)
      throws IOException {

    BinaryDocValues textDV = MultiDocValues.getBinaryValues(searcher.getIndexReader(), TEXT_FIELD_NAME);

    // This will just be null if app didn't pass payloads to build():
    // TODO: maybe just stored fields?  they compress...
    BinaryDocValues payloadsDV = MultiDocValues.getBinaryValues(searcher.getIndexReader(), "payloads");
    List<AtomicReaderContext> leaves = searcher.getIndexReader().leaves();
    List<LookupResult> results = new ArrayList<>();
    for (int i=0;i<hits.scoreDocs.length;i++) {
      FieldDoc fd = (FieldDoc) hits.scoreDocs[i];
      BytesRef term = textDV.get(fd.doc);
      String text = term.utf8ToString();
      long score = (Long) fd.fields[0];

      BytesRef payload;
      if (payloadsDV != null) {
        payload = BytesRef.deepCopyOf(payloadsDV.get(fd.doc));
      } else {
        payload = null;
      }

      // Must look up sorted-set by segment:
      int segment = ReaderUtil.subIndex(fd.doc, leaves);
      SortedSetDocValues contextsDV = leaves.get(segment).reader().getSortedSetDocValues(CONTEXTS_FIELD_NAME);
      Set<BytesRef> contexts;
      if (contextsDV != null) {
        contexts = new HashSet<BytesRef>();
        contextsDV.setDocument(fd.doc - leaves.get(segment).docBase);
        long ord;
        while ((ord = contextsDV.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
          BytesRef context = BytesRef.deepCopyOf(contextsDV.lookupOrd(ord));
          contexts.add(context);
        }
      } else {
        contexts = null;
      }

      LookupResult result;

      if (doHighlight) {
        Object highlightKey = highlight(text, matchedTokens, prefixToken);
        result = new LookupResult(highlightKey.toString(), highlightKey, score, payload, contexts);
      } else {
        result = new LookupResult(text, score, payload, contexts);
      }

      results.add(result);
    }

    return results;
  }

  /** Subclass can override this to tweak the Query before
   *  searching. */
  protected Query finishQuery(BooleanQuery in, boolean allTermsRequired) {
    return in;
  }

  /** Override this method to customize the Object
   *  representing a single highlighted suggestions; the
   *  result is set on each {@link
   *  LookupResult#highlightKey} member. */
  protected Object highlight(String text, Set<String> matchedTokens, String prefixToken) throws IOException {
    TokenStream ts = queryAnalyzer.tokenStream("text", new StringReader(text));
    try {
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
    } finally {
      IOUtils.closeWhileHandlingException(ts);
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
    sb.append("<b>");
    sb.append(surface.substring(0, prefixToken.length()));
    sb.append("</b>");
    if (prefixToken.length() < surface.length()) {
      sb.append(surface.substring(prefixToken.length()));
    }
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
      dir.close();
      writer = null;
    }
  }

  @Override
  public long ramBytesUsed() {
    long mem = RamUsageEstimator.shallowSizeOf(this);
    try {
      if (searcherMgr != null) {
        IndexSearcher searcher = searcherMgr.acquire();
        try {
          for (AtomicReaderContext context : searcher.getIndexReader().leaves()) {
            AtomicReader reader = FilterAtomicReader.unwrap(context.reader());
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
  public long getCount() throws IOException {
    IndexSearcher searcher = searcherMgr.acquire();
    try {
      return searcher.getIndexReader().numDocs();
    } finally {
      searcherMgr.release(searcher);
    }
  }
};
