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
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.AnalyzerWrapper;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.ngram.EdgeNGramTokenFilter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.codecs.lucene46.Lucene46Codec;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.sorter.Sorter;
import org.apache.lucene.index.sorter.SortingAtomicReader;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.suggest.Lookup.LookupResult; // javadocs
import org.apache.lucene.search.suggest.InputIterator;
import org.apache.lucene.search.suggest.Lookup;
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
//   - we could allow NRT here, if we sort index as we go
//     (SortingMergePolicy) -- http://svn.apache.org/viewvc?view=revision&revision=1459808

/** Analyzes the input text and then suggests matches based
 *  on prefix matches to any tokens in the indexed text.
 *  This also highlights the tokens that match.
 *
 *  <p>This just uses an ordinary Lucene index.  It
 *  supports payloads, and records these as a
 *  {@link BinaryDocValues} field.  Matches are sorted only
 *  by the suggest weight; it would be nice to support
 *  blended score + weight sort in the future.  This means
 *  this suggester best applies when there is a strong
 *  apriori ranking of all the suggestions. */

public class AnalyzingInfixSuggester extends Lookup implements Closeable {

  /** Field name used for the indexed text. */
  protected final static String TEXT_FIELD_NAME = "text";

  /** Analyzer used at search time */
  protected final Analyzer queryAnalyzer;
  /** Analyzer used at index time */
  protected final Analyzer indexAnalyzer;
  final Version matchVersion;
  private final File indexPath;
  final int minPrefixChars;
  private Directory dir;

  /** {@link IndexSearcher} used for lookups. */
  protected IndexSearcher searcher;

  /** null if payloads were not indexed: */
  private BinaryDocValues payloadsDV;
  private BinaryDocValues textDV;
  private NumericDocValues weightsDV;

  /** Default minimum number of leading characters before
   *  PrefixQuery is used (4). */
  public static final int DEFAULT_MIN_PREFIX_CHARS = 4;

  /** Create a new instance, loading from a previously built
   *  directory, if it exists. */
  public AnalyzingInfixSuggester(Version matchVersion, File indexPath, Analyzer analyzer) throws IOException {
    this(matchVersion, indexPath, analyzer, analyzer, DEFAULT_MIN_PREFIX_CHARS);
  }

  /** Create a new instance, loading from a previously built
   *  directory, if it exists.
   *
   *  @param minPrefixChars Minimum number of leading characters
   *     before PrefixQuery is used (default 4).
   *     Prefixes shorter than this are indexed as character
   *     ngrams (increasing index size but making lookups
   *     faster).
   */
  public AnalyzingInfixSuggester(Version matchVersion, File indexPath, Analyzer indexAnalyzer, Analyzer queryAnalyzer, int minPrefixChars) throws IOException {

    if (minPrefixChars < 0) {
      throw new IllegalArgumentException("minPrefixChars must be >= 0; got: " + minPrefixChars);
    }

    this.queryAnalyzer = queryAnalyzer;
    this.indexAnalyzer = indexAnalyzer;
    this.matchVersion = matchVersion;
    this.indexPath = indexPath;
    this.minPrefixChars = minPrefixChars;
    dir = getDirectory(indexPath);

    if (DirectoryReader.indexExists(dir)) {
      // Already built; open it:
      searcher = new IndexSearcher(DirectoryReader.open(dir));
      // This will just be null if app didn't pass payloads to build():
      // TODO: maybe just stored fields?  they compress...
      payloadsDV = MultiDocValues.getBinaryValues(searcher.getIndexReader(), "payloads");
      weightsDV = MultiDocValues.getNumericValues(searcher.getIndexReader(), "weight");
      textDV = MultiDocValues.getBinaryValues(searcher.getIndexReader(), TEXT_FIELD_NAME);
      assert textDV != null;
    }
  }

  /** Override this to customize index settings, e.g. which
   *  codec to use. */
  protected IndexWriterConfig getIndexWriterConfig(Version matchVersion, Analyzer indexAnalyzer) {
    IndexWriterConfig iwc = new IndexWriterConfig(matchVersion, indexAnalyzer);
    iwc.setCodec(new Lucene46Codec());
    iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
    return iwc;
  }

  /** Subclass can override to choose a specific {@link
   *  Directory} implementation. */
  protected Directory getDirectory(File path) throws IOException {
    return FSDirectory.open(path);
  }

  @Override
  public void build(InputIterator iter) throws IOException {

    if (searcher != null) {
      searcher.getIndexReader().close();
      searcher = null;
    }


    Directory dirTmp = getDirectory(new File(indexPath.toString() + ".tmp"));

    IndexWriter w = null;
    IndexWriter w2 = null;
    AtomicReader r = null;
    boolean success = false;
    try {
      Analyzer gramAnalyzer = new AnalyzerWrapper(Analyzer.PER_FIELD_REUSE_STRATEGY) {
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

      w = new IndexWriter(dirTmp,
                          getIndexWriterConfig(matchVersion, gramAnalyzer));
      BytesRef text;
      Document doc = new Document();
      FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
      ft.setIndexOptions(IndexOptions.DOCS_ONLY);
      ft.setOmitNorms(true);
      Field textField = new Field(TEXT_FIELD_NAME, "", ft);
      doc.add(textField);

      Field textGramField = new Field("textgrams", "", ft);
      doc.add(textGramField);

      Field textDVField = new BinaryDocValuesField(TEXT_FIELD_NAME, new BytesRef());
      doc.add(textDVField);

      // TODO: use threads...?
      Field weightField = new NumericDocValuesField("weight", 0);
      doc.add(weightField);

      Field payloadField;
      if (iter.hasPayloads()) {
        payloadField = new BinaryDocValuesField("payloads", new BytesRef());
        doc.add(payloadField);
      } else {
        payloadField = null;
      }

      //long t0 = System.nanoTime();
      while ((text = iter.next()) != null) {
        String textString = text.utf8ToString();
        textField.setStringValue(textString);
        textGramField.setStringValue(textString);
        textDVField.setBytesValue(text);
        weightField.setLongValue(iter.weight());
        if (iter.hasPayloads()) {
          payloadField.setBytesValue(iter.payload());
        }
        w.addDocument(doc);
      }
      //System.out.println("initial indexing time: " + ((System.nanoTime()-t0)/1000000) + " msec");

      r = SlowCompositeReaderWrapper.wrap(DirectoryReader.open(w, false));
      //long t1 = System.nanoTime();
      w.rollback();

      final int maxDoc = r.maxDoc();

      final NumericDocValues weights = r.getNumericDocValues("weight");

      final Sorter.DocComparator comparator = new Sorter.DocComparator() {
          @Override
          public int compare(int docID1, int docID2) {
            final long v1 = weights.get(docID1);
            final long v2 = weights.get(docID2);
            // Reverse sort (highest weight first);
            // java7 only:
            //return Long.compare(v2, v1);
            if (v1 > v2) {
              return -1;
            } else if (v1 < v2) {
              return 1;
            } else {
              return 0;
            }
          }
        };

      r = SortingAtomicReader.wrap(r, new Sorter() {
          @Override
          public Sorter.DocMap sort(AtomicReader reader) throws IOException {
            return Sorter.sort(maxDoc, comparator);
          }

          @Override
          public String getID() {
            return "Weight";
          }
        });
      
      w2 = new IndexWriter(dir,
                           getIndexWriterConfig(matchVersion, indexAnalyzer));
      w2.addIndexes(new IndexReader[] {r});
      r.close();

      //System.out.println("sort time: " + ((System.nanoTime()-t1)/1000000) + " msec");

      searcher = new IndexSearcher(DirectoryReader.open(w2, false));
      w2.close();

      payloadsDV = MultiDocValues.getBinaryValues(searcher.getIndexReader(), "payloads");
      weightsDV = MultiDocValues.getNumericValues(searcher.getIndexReader(), "weight");
      textDV = MultiDocValues.getBinaryValues(searcher.getIndexReader(), TEXT_FIELD_NAME);
      assert textDV != null;
      success = true;
    } finally {
      if (success) {
        IOUtils.close(w, w2, r, dirTmp);
      } else {
        IOUtils.closeWhileHandlingException(w, w2, r, dirTmp);
      }
    }
  }

  @Override
  public List<LookupResult> lookup(CharSequence key, boolean onlyMorePopular, int num) {
    return lookup(key, num, true, true);
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
  public List<LookupResult> lookup(CharSequence key, int num, boolean allTermsRequired, boolean doHighlight) {

    if (searcher == null) {
      throw new IllegalStateException("suggester was not built");
    }

    final BooleanClause.Occur occur;
    if (allTermsRequired) {
      occur = BooleanClause.Occur.MUST;
    } else {
      occur = BooleanClause.Occur.SHOULD;
    }

    TokenStream ts = null;
    try {
      ts = queryAnalyzer.tokenStream("", new StringReader(key.toString()));
      //long t0 = System.currentTimeMillis();
      ts.reset();
      final CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
      final OffsetAttribute offsetAtt = ts.addAttribute(OffsetAttribute.class);
      String lastToken = null;
      BooleanQuery query = new BooleanQuery();
      int maxEndOffset = -1;
      final Set<String> matchedTokens = new HashSet<String>();
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

      String prefixToken = null;
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
      ts.close();

      // TODO: we could allow blended sort here, combining
      // weight w/ score.  Now we ignore score and sort only
      // by weight:

      //System.out.println("INFIX query=" + query);

      Query finalQuery = finishQuery(query, allTermsRequired);

      // We sorted postings by weight during indexing, so we
      // only retrieve the first num hits now:
      FirstNDocsCollector c = new FirstNDocsCollector(num);
      try {
        searcher.search(finalQuery, c);
      } catch (FirstNDocsCollector.DoneException done) {
      }
      TopDocs hits = c.getHits();

      // Slower way if postings are not pre-sorted by weight:
      // hits = searcher.search(query, null, num, new Sort(new SortField("weight", SortField.Type.LONG, true)));

      List<LookupResult> results = new ArrayList<LookupResult>();
      BytesRef scratch = new BytesRef();
      for (int i=0;i<hits.scoreDocs.length;i++) {
        ScoreDoc sd = hits.scoreDocs[i];
        textDV.get(sd.doc, scratch);
        String text = scratch.utf8ToString();
        long score = weightsDV.get(sd.doc);

        BytesRef payload;
        if (payloadsDV != null) {
          payload = new BytesRef();
          payloadsDV.get(sd.doc, payload);
        } else {
          payload = null;
        }

        LookupResult result;

        if (doHighlight) {
          Object highlightKey = highlight(text, matchedTokens, prefixToken);
          result = new LookupResult(highlightKey.toString(), highlightKey, score, payload);
        } else {
          result = new LookupResult(text, score, payload);
        }
        results.add(result);
      }
      //System.out.println((System.currentTimeMillis() - t0) + " msec for infix suggest");
      //System.out.println(results);
      return results;
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    } finally {
      IOUtils.closeWhileHandlingException(ts);
    }
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

  private static class FirstNDocsCollector extends Collector {
    private int docBase;
    private final int[] hits;
    private int hitCount;

    private static class DoneException extends RuntimeException {
    }

    public TopDocs getHits() {
      ScoreDoc[] scoreDocs = new ScoreDoc[hitCount];
      for(int i=0;i<hitCount;i++) {
        scoreDocs[i] = new ScoreDoc(hits[i], Float.NaN);
      }
      return new TopDocs(hitCount, scoreDocs, Float.NaN);
    }

    public FirstNDocsCollector(int topN) {
      hits = new int[topN];
    }

    @Override
    public void collect(int doc) {
      //System.out.println("collect doc=" + doc);
      hits[hitCount++] = doc;
      if (hitCount == hits.length) {
        throw new DoneException();
      }
    }

    @Override
    public void setScorer(Scorer scorer) {
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
      return false;
    }

    @Override
    public void setNextReader(AtomicReaderContext cxt) {
      docBase = cxt.docBase;
    }
  }

  @Override
  public boolean store(OutputStream out) {
    return false;
  }

  @Override
  public boolean load(InputStream out) {
    return false;
  }

  @Override
  public void close() throws IOException {
    if (searcher != null) {
      searcher.getIndexReader().close();
      searcher = null;
    }
    if (dir != null) {
      dir.close();
      dir = null;
    }
  }

  @Override
  public long sizeInBytes() {
    return RamUsageEstimator.sizeOf(this);
  }
};
