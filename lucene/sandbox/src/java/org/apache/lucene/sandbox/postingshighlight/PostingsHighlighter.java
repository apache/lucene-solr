package org.apache.lucene.sandbox.postingshighlight;

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

import java.io.IOException;
import java.text.BreakIterator;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BytesRef;

/**
 * Simple highlighter that does not analyze fields nor use
 * term vectors. Instead it requires 
 * {@link IndexOptions#DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS}.
 * 
 * This is thread-safe, and can be used across different readers.
 * <pre class="prettyprint">
 *   // configure field with offsets at index time
 *   FieldType offsetsType = new FieldType(TextField.TYPE_STORED);
 *   offsetsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
 *   Field body = new Field("body", "foobar", offsetsType);
 *
 *   // retrieve highlights at query time 
 *   PostingsHighlighter highlighter = new PostingsHighlighter("body");
 *   Query query = new TermQuery(new Term("body", "highlighting"));
 *   TopDocs topDocs = searcher.search(query, n);
 *   String highlights[] = highlighter.highlight(query, searcher, topDocs);
 * </pre>
 * @lucene.experimental
 */
public final class PostingsHighlighter {
  
  // TODO: support highlighting multiple fields at once? someone is bound
  // to try to use this in a slow way (invoking over and over for each field), which
  // would be horrible.
  
  // TODO: maybe allow re-analysis for tiny fields? currently we require offsets,
  // but if the analyzer is really fast and the field is tiny, this might really be
  // unnecessary.
  
  /** for rewriting: we don't want slow processing from MTQs */
  private static final IndexReader EMPTY_INDEXREADER = new MultiReader();
  
  /** Default maximum content size to process. Typically snippets
   *  closer to the beginning of the document better summarize its content */
  public static final int DEFAULT_MAX_LENGTH = 10000;
  
  // this looks bogus, but its not. we are dealing with characters :)
  private static final BytesRef ceilingBytes = new BytesRef(new byte[] { (byte)0xff });  
  private final String field;
  private final Term floor;
  private final Term ceiling;
  private final int maxLength;
  private final BreakIterator breakIterator;
  private final PassageScorer scorer;
  private final PassageFormatter formatter;
  
  public PostingsHighlighter(String field) {
    this(field, DEFAULT_MAX_LENGTH);
  }
  
  public PostingsHighlighter(String field, int maxLength) {
    this(field, maxLength, BreakIterator.getSentenceInstance(Locale.ROOT), new PassageScorer(), new PassageFormatter());
  }
  
  public PostingsHighlighter(String field, int maxLength, BreakIterator breakIterator, PassageScorer scorer, PassageFormatter formatter) {
    this.field = field;
    if (maxLength == Integer.MAX_VALUE) {
      // two reasons: no overflow problems in BreakIterator.preceding(offset+1),
      // our sentinel in the offsets queue uses this value to terminate.
      throw new IllegalArgumentException("maxLength must be < Integer.MAX_VALUE");
    }
    this.maxLength = maxLength;
    this.breakIterator = breakIterator;
    this.scorer = scorer;
    this.formatter = formatter;
    floor = new Term(field, "");
    ceiling = new Term(field, ceilingBytes);
  }
  
  /**
   * Calls {@link #highlight(Query, IndexSearcher, TopDocs, int) highlight(query, searcher, topDocs, 1)}
   */
  public String[] highlight(Query query, IndexSearcher searcher, TopDocs topDocs) throws IOException {
    return highlight(query, searcher, topDocs, 1);
  }
  
  public String[] highlight(Query query, IndexSearcher searcher, TopDocs topDocs, int maxPassages) throws IOException {
    final IndexReader reader = searcher.getIndexReader();
    final ScoreDoc scoreDocs[] = topDocs.scoreDocs;
    query = rewrite(query);
    SortedSet<Term> terms = new TreeSet<Term>();
    query.extractTerms(terms);
    terms = terms.subSet(floor, ceiling);
    // TODO: should we have some reasonable defaults for term pruning? (e.g. stopwords)

    int docids[] = new int[scoreDocs.length];
    for (int i = 0; i < docids.length; i++) {
      docids[i] = scoreDocs[i].doc;
    }
    IndexReaderContext readerContext = reader.getContext();
    List<AtomicReaderContext> leaves = readerContext.leaves();
    
    // sort for sequential io
    Arrays.sort(docids);
    
    // pull stored data
    LimitedStoredFieldVisitor visitor = new LimitedStoredFieldVisitor(field, maxLength);
    String contents[] = new String[docids.length];
    for (int i = 0; i < contents.length; i++) {
      reader.document(docids[i], visitor);
      contents[i] = visitor.getValue();
      visitor.reset();
    }
    
    // now pull index stats: TODO: we should probably pull this from the reader instead?
    // this could be a distributed call, which is crazy
    CollectionStatistics collectionStats = searcher.collectionStatistics(field);
    TermContext termContexts[] = new TermContext[terms.size()];
    Term termTexts[] = new Term[terms.size()]; // needed for seekExact
    float weights[] = new float[terms.size()];
    int upto = 0;
    for (Term term : terms) {
      termTexts[upto] = term;
      TermContext context = TermContext.build(readerContext, term, true);
      termContexts[upto] = context;
      TermStatistics termStats = searcher.termStatistics(term, context);
      weights[upto] = scorer.weight(collectionStats, termStats);
      upto++;
      // TODO: should we instead score all the documents term-at-a-time here?
      // the i/o would be better, but more transient ram
    }
    
    BreakIterator bi = (BreakIterator)breakIterator.clone();
    
    Map<Integer,String> highlights = new HashMap<Integer,String>();
    
    // reuse in the real sense... for docs in same segment we just advance our old enum
    DocsAndPositionsEnum postings[] = null;
    TermsEnum termsEnum = null;
    int lastLeaf = -1;
    
    for (int i = 0; i < docids.length; i++) {
      String content = contents[i];
      if (content.length() == 0) {
        continue; // nothing to do
      }
      bi.setText(content);
      int doc = docids[i];
      int leaf = ReaderUtil.subIndex(doc, leaves);
      AtomicReaderContext subContext = leaves.get(leaf);
      AtomicReader r = subContext.reader();
      Terms t = r.terms(field);
      if (t == null) {
        continue; // nothing to do
      }
      if (leaf != lastLeaf) {
        termsEnum = t.iterator(null);
        postings = new DocsAndPositionsEnum[terms.size()];
      }
      Passage passages[] = highlightDoc(termTexts, termContexts, subContext.ord, weights, content.length(), bi, doc - subContext.docBase, termsEnum, postings, maxPassages);
      if (passages.length > 0) {
        // otherwise a null snippet
        highlights.put(doc, formatter.format(passages, content));
      }
      lastLeaf = leaf;
    }
    
    String[] result = new String[scoreDocs.length];
    for (int i = 0; i < scoreDocs.length; i++) {
      result[i] = highlights.get(scoreDocs[i].doc);
    }
    return result;
  }
  
  // algorithm: treat sentence snippets as miniature documents
  // we can intersect these with the postings lists via BreakIterator.preceding(offset),s
  // score each sentence as norm(sentenceStartOffset) * sum(weight * tf(freq))
  private Passage[] highlightDoc(Term termTexts[], TermContext[] terms, int ord, float[] weights, 
      int contentLength, BreakIterator bi, int doc, TermsEnum termsEnum, DocsAndPositionsEnum[] postings, int n) throws IOException {
    PriorityQueue<OffsetsEnum> pq = new PriorityQueue<OffsetsEnum>();
    // initialize postings
    for (int i = 0; i < terms.length; i++) {
      DocsAndPositionsEnum de = postings[i];
      int pDoc;
      if (de == EMPTY) {
        continue;
      } else if (de == null) {
        postings[i] = EMPTY; // initially
        TermState ts = terms[i].get(ord);
        if (ts == null) {
          continue;
        }
        termsEnum.seekExact(termTexts[i].bytes(), ts);
        DocsAndPositionsEnum de2 = termsEnum.docsAndPositions(null, null, DocsAndPositionsEnum.FLAG_OFFSETS);
        if (de2 == null) {
          continue;
        } else {
          de = postings[i] = de2;
        }
        pDoc = de.advance(doc);
      } else {
        pDoc = de.docID();
        if (pDoc < doc) {
          pDoc = de.advance(doc);
        }
      }

      if (doc == pDoc) {
        de.nextPosition();
        pq.add(new OffsetsEnum(de, i));
      }
    }
    
    pq.add(new OffsetsEnum(EMPTY, Integer.MAX_VALUE)); // a sentinel for termination
    
    PriorityQueue<Passage> passageQueue = new PriorityQueue<Passage>(n, new Comparator<Passage>() {
      @Override
      public int compare(Passage left, Passage right) {
        if (right.score == left.score) {
          return right.startOffset - left.endOffset;
        } else {
          return right.score > left.score ? 1 : -1;
        }
      }
    });
    Passage current = new Passage();
    
    OffsetsEnum off;
    while ((off = pq.poll()) != null) {
      final DocsAndPositionsEnum dp = off.dp;
      int start = dp.startOffset();
      if (start == -1) {
        throw new IllegalArgumentException("field '" + field + "' was indexed without offsets, cannot highlight");
      }
      int end = dp.endOffset();
      if (start > current.endOffset) {
        if (current.startOffset >= 0) {
          // finalize current
          current.score *= scorer.norm(current.startOffset);
          // new sentence: first add 'current' to queue 
          if (passageQueue.size() == n && current.score < passageQueue.peek().score) {
            current.reset(); // can't compete, just reset it
          } else {
            passageQueue.offer(current);
            if (passageQueue.size() > n) {
              current = passageQueue.poll();
              current.reset();
            } else {
              current = new Passage();
            }
          }
        }
        // if we exceed limit, we are done
        if (start >= contentLength) {
          Passage passages[] = new Passage[passageQueue.size()];
          passageQueue.toArray(passages);
          // sort in ascending order
          Arrays.sort(passages, new Comparator<Passage>() {
            @Override
            public int compare(Passage left, Passage right) {
              return left.startOffset - right.startOffset;
            }
          });
          return passages;
        }
        // advance breakiterator
        assert BreakIterator.DONE < 0;
        current.startOffset = Math.max(bi.preceding(start+1), 0);
        current.endOffset = Math.min(bi.next(), contentLength);
      }
      int tf = 0;
      while (true) {
        tf++;
        current.addMatch(start, end, termTexts[off.id]);
        if (off.pos == dp.freq()) {
          break; // removed from pq
        } else {
          off.pos++;
          dp.nextPosition();
          start = dp.startOffset();
          end = dp.endOffset();
        }
        if (start >= current.endOffset) {
          pq.offer(off);
          break;
        }
      }
      current.score += weights[off.id] * scorer.tf(tf, current.endOffset - current.startOffset);
    }
    return new Passage[0];
  }
  
  private static class OffsetsEnum implements Comparable<OffsetsEnum> {
    DocsAndPositionsEnum dp;
    int pos;
    int id;
    
    OffsetsEnum(DocsAndPositionsEnum dp, int id) throws IOException {
      this.dp = dp;
      this.id = id;
      this.pos = 1;
    }

    @Override
    public int compareTo(OffsetsEnum other) {
      try {
        int off = dp.startOffset();
        int otherOff = other.dp.startOffset();
        if (off == otherOff) {
          return id - other.id;
        } else {
          return Long.signum(((long)off) - otherOff);
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
  
  private static final DocsAndPositionsEnum EMPTY = new DocsAndPositionsEnum() {

    @Override
    public int nextPosition() throws IOException { return 0; }

    @Override
    public int startOffset() throws IOException { return Integer.MAX_VALUE; }

    @Override
    public int endOffset() throws IOException { return Integer.MAX_VALUE; }

    @Override
    public BytesRef getPayload() throws IOException { return null; }

    @Override
    public int freq() throws IOException { return 0; }

    @Override
    public int docID() { return NO_MORE_DOCS; }

    @Override
    public int nextDoc() throws IOException { return NO_MORE_DOCS; }

    @Override
    public int advance(int target) throws IOException { return NO_MORE_DOCS; }
  };
  
  /** 
   * we rewrite against an empty indexreader: as we don't want things like
   * rangeQueries that don't summarize the document
   */
  private static Query rewrite(Query original) throws IOException {
    Query query = original;
    for (Query rewrittenQuery = query.rewrite(EMPTY_INDEXREADER); rewrittenQuery != query;
         rewrittenQuery = query.rewrite(EMPTY_INDEXREADER)) {
      query = rewrittenQuery;
    }
    return query;
  }
  
  private static class LimitedStoredFieldVisitor extends StoredFieldVisitor {
    private final String field;
    private final int maxLength;
    private final StringBuilder builder = new StringBuilder();
    
    public LimitedStoredFieldVisitor(String field, int maxLength) {
      this.field = field;
      this.maxLength = maxLength;
    }
    
    @Override
    public void stringField(FieldInfo fieldInfo, String value) throws IOException {
      if (builder.length() > 0) {
        builder.append(' '); // for the offset gap, TODO: make this configurable
      }
      if (builder.length() + value.length() > maxLength) {
        builder.append(value, 0, maxLength - builder.length());
      } else {
        builder.append(value);
      }
    }

    @Override
    public Status needsField(FieldInfo fieldInfo) throws IOException {
      if (fieldInfo.name.equals(field)) {
        if (builder.length() > maxLength) {
          return Status.STOP;
        }
        return Status.YES;
      } else {
        return Status.NO;
      }
    }
    
    String getValue() {
      return builder.toString();
    }
    
    void reset() {
      builder.setLength(0);
    }
  }
}
