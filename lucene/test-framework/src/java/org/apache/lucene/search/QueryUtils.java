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
package org.apache.lucene.search;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.Terms;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.LuceneTestCase;

import junit.framework.Assert;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;

/**
 * Utility class for sanity-checking queries.
 */
public class QueryUtils {

  /** Check the types of things query objects should be able to do. */
  public static void check(Query q) {
    checkHashEquals(q);
  }

  /** check very basic hashCode and equals */
  public static void checkHashEquals(Query q) {
    checkEqual(q,q);

    // test that a class check is done so that no exception is thrown
    // in the implementation of equals()
    Query whacky = new Query() {
      @Override
      public String toString(String field) {
        return "My Whacky Query";
      }

      @Override
      public boolean equals(Object o) {
        return o == this;
      }

      @Override
      public int hashCode() {
        return System.identityHashCode(this);
      }

    };
    checkUnequal(q, whacky);

    // null test
    assertFalse(q.equals(null));
  }

  public static void checkEqual(Query q1, Query q2) {
    assertEquals(q1, q2);
    assertEquals(q1.hashCode(), q2.hashCode());
  }

  public static void checkUnequal(Query q1, Query q2) {
    assertFalse(q1 + " equal to " + q2, q1.equals(q2));
    assertFalse(q2 + " equal to " + q1, q2.equals(q1));
  }

  /** deep check that explanations of a query 'score' correctly */
  public static void checkExplanations (final Query q, final IndexSearcher s) throws IOException {
    CheckHits.checkExplanations(q, null, s, true);
  }

  /**
   * Various query sanity checks on a searcher, some checks are only done for
   * instanceof IndexSearcher.
   *
   * @see #check(Query)
   * @see #checkFirstSkipTo
   * @see #checkSkipTo
   * @see #checkExplanations
   * @see #checkEqual
   */
  public static void check(Random random, Query q1, IndexSearcher s) {
    check(random, q1, s, true);
  }
  public static void check(Random random, Query q1, IndexSearcher s, boolean wrap) {
    try {
      check(q1);
      if (s!=null) {
        checkFirstSkipTo(q1,s);
        checkSkipTo(q1,s);
        checkBulkScorerSkipTo(random, q1, s);
        if (wrap) {
          check(random, q1, wrapUnderlyingReader(random, s, -1), false);
          check(random, q1, wrapUnderlyingReader(random, s,  0), false);
          check(random, q1, wrapUnderlyingReader(random, s, +1), false);
        }
        checkExplanations(q1,s);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /** This is a MultiReader that can be used for randomly wrapping other readers
   * without creating FieldCache insanity.
   * The trick is to use an opaque/fake cache key. */
  public static class FCInvisibleMultiReader extends MultiReader {
    private final Object cacheKey = new Object();

    public FCInvisibleMultiReader(IndexReader... readers) throws IOException {
      super(readers);
    }

    @Override
    public Object getCoreCacheKey() {
      return cacheKey;
    }

    @Override
    public Object getCombinedCoreAndDeletesKey() {
      return cacheKey;
    }
  }

  /**
   * Given an IndexSearcher, returns a new IndexSearcher whose IndexReader
   * is a MultiReader containing the Reader of the original IndexSearcher,
   * as well as several "empty" IndexReaders -- some of which will have
   * deleted documents in them.  This new IndexSearcher should
   * behave exactly the same as the original IndexSearcher.
   * @param s the searcher to wrap
   * @param edge if negative, s will be the first sub; if 0, s will be in the middle, if positive s will be the last sub
   */
  public static IndexSearcher wrapUnderlyingReader(Random random, final IndexSearcher s, final int edge)
    throws IOException {

    IndexReader r = s.getIndexReader();

    // we can't put deleted docs before the nested reader, because
    // it will throw off the docIds
    IndexReader[] readers = new IndexReader[] {
      edge < 0 ? r : new MultiReader(),
      new MultiReader(),
      new FCInvisibleMultiReader(edge < 0 ? emptyReader(4) : new MultiReader(),
          new MultiReader(),
          0 == edge ? r : new MultiReader()),
      0 < edge ? new MultiReader() : emptyReader(7),
      new MultiReader(),
      new FCInvisibleMultiReader(0 < edge ? new MultiReader() : emptyReader(5),
          new MultiReader(),
          0 < edge ? r : new MultiReader())
    };

    IndexSearcher out = LuceneTestCase.newSearcher(new FCInvisibleMultiReader(readers));
    out.setSimilarity(s.getSimilarity(true));
    return out;
  }

  private static IndexReader emptyReader(final int maxDoc) {
    return new LeafReader() {

      @Override
      public void addCoreClosedListener(CoreClosedListener listener) {}

      @Override
      public void removeCoreClosedListener(CoreClosedListener listener) {}

      @Override
      public Fields fields() throws IOException {
        return new Fields() {
          @Override
          public Iterator<String> iterator() {
            return Collections.<String>emptyList().iterator();
          }

          @Override
          public Terms terms(String field) throws IOException {
            return null;
          }

          @Override
          public int size() {
            return 0;
          }
        };
      }

      @Override
      public NumericDocValues getNumericDocValues(String field) throws IOException {
        return null;
      }

      @Override
      public BinaryDocValues getBinaryDocValues(String field) throws IOException {
        return null;
      }

      @Override
      public SortedDocValues getSortedDocValues(String field) throws IOException {
        return null;
      }

      @Override
      public SortedNumericDocValues getSortedNumericDocValues(String field) throws IOException {
        return null;
      }

      @Override
      public SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
        return null;
      }

      @Override
      public NumericDocValues getNormValues(String field) throws IOException {
        return null;
      }

      @Override
      public FieldInfos getFieldInfos() {
        return new FieldInfos(new FieldInfo[0]);
      }

      final Bits liveDocs = new Bits.MatchNoBits(maxDoc);
      @Override
      public Bits getLiveDocs() {
        return liveDocs;
      }

      @Override
      public PointValues getPointValues(String fieldName) {
        return null;
      }

      @Override
      public void checkIntegrity() throws IOException {}

      @Override
      public Fields getTermVectors(int docID) throws IOException {
        return null;
      }

      @Override
      public int numDocs() {
        return 0;
      }

      @Override
      public int maxDoc() {
        return maxDoc;
      }

      @Override
      public void document(int docID, StoredFieldVisitor visitor) throws IOException {}

      @Override
      protected void doClose() throws IOException {}

      @Override
      public Sort getIndexSort() {
        return null;
      }
    };
  }

  /** alternate scorer advance(),advance(),next(),next(),advance(),advance(), etc
   * and ensure a hitcollector receives same docs and scores
   */
  public static void checkSkipTo(final Query q, final IndexSearcher s) throws IOException {
    //System.out.println("Checking "+q);
    final List<LeafReaderContext> readerContextArray = s.getTopReaderContext().leaves();

    final int skip_op = 0;
    final int next_op = 1;
    final int orders [][] = {
        {next_op},
        {skip_op},
        {skip_op, next_op},
        {next_op, skip_op},
        {skip_op, skip_op, next_op, next_op},
        {next_op, next_op, skip_op, skip_op},
        {skip_op, skip_op, skip_op, next_op, next_op},
    };
    for (int k = 0; k < orders.length; k++) {

        final int order[] = orders[k];
        // System.out.print("Order:");for (int i = 0; i < order.length; i++)
        // System.out.print(order[i]==skip_op ? " skip()":" next()");
        // System.out.println();
        final int opidx[] = { 0 };
        final int lastDoc[] = {-1};

        // FUTURE: ensure scorer.doc()==-1

        final float maxDiff = 1e-5f;
        final LeafReader lastReader[] = {null};

        s.search(q, new SimpleCollector() {
          private Scorer sc;
          private Scorer scorer;
          private DocIdSetIterator iterator;
          private int leafPtr;

          @Override
          public void setScorer(Scorer scorer) {
            this.sc = scorer;
          }

          @Override
          public void collect(int doc) throws IOException {
            float score = sc.score();
            lastDoc[0] = doc;
            try {
              if (scorer == null) {
                Weight w = s.createNormalizedWeight(q, true);
                LeafReaderContext context = readerContextArray.get(leafPtr);
                scorer = w.scorer(context);
                iterator = scorer.iterator();
              }

              int op = order[(opidx[0]++) % order.length];
              // System.out.println(op==skip_op ?
              // "skip("+(sdoc[0]+1)+")":"next()");
              boolean more = op == skip_op ? iterator.advance(scorer.docID() + 1) != DocIdSetIterator.NO_MORE_DOCS
                  : iterator.nextDoc() != DocIdSetIterator.NO_MORE_DOCS;
              int scorerDoc = scorer.docID();
              float scorerScore = scorer.score();
              float scorerScore2 = scorer.score();
              float scoreDiff = Math.abs(score - scorerScore);
              float scorerDiff = Math.abs(scorerScore2 - scorerScore);

              boolean success = false;
              try {
                assertTrue(more);
                assertEquals("scorerDoc=" + scorerDoc + ",doc=" + doc, scorerDoc, doc);
                assertTrue("score=" + score + ", scorerScore=" + scorerScore, scoreDiff <= maxDiff);
                assertTrue("scorerScorer=" + scorerScore + ", scorerScore2=" + scorerScore2, scorerDiff <= maxDiff);
                success = true;
              } finally {
                if (!success) {
                  if (LuceneTestCase.VERBOSE) {
                    StringBuilder sbord = new StringBuilder();
                    for (int i = 0; i < order.length; i++) {
                      sbord.append(order[i] == skip_op ? " skip()" : " next()");
                    }
                    System.out.println("ERROR matching docs:" + "\n\t"
                        + (doc != scorerDoc ? "--> " : "") + "doc=" + doc + ", scorerDoc=" + scorerDoc
                        + "\n\t" + (!more ? "--> " : "") + "tscorer.more=" + more
                        + "\n\t" + (scoreDiff > maxDiff ? "--> " : "")
                        + "scorerScore=" + scorerScore + " scoreDiff=" + scoreDiff
                        + " maxDiff=" + maxDiff + "\n\t"
                        + (scorerDiff > maxDiff ? "--> " : "") + "scorerScore2="
                        + scorerScore2 + " scorerDiff=" + scorerDiff
                        + "\n\thitCollector.doc=" + doc + " score=" + score
                        + "\n\t Scorer=" + scorer + "\n\t Query=" + q + "  "
                        + q.getClass().getName() + "\n\t Searcher=" + s
                        + "\n\t Order=" + sbord + "\n\t Op="
                        + (op == skip_op ? " skip()" : " next()"));
                  }
                }
              }
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }

          @Override
          public boolean needsScores() {
            return true;
          }

          @Override
          protected void doSetNextReader(LeafReaderContext context) throws IOException {
            // confirm that skipping beyond the last doc, on the
            // previous reader, hits NO_MORE_DOCS
            if (lastReader[0] != null) {
              final LeafReader previousReader = lastReader[0];
              IndexSearcher indexSearcher = LuceneTestCase.newSearcher(previousReader, false);
              indexSearcher.setSimilarity(s.getSimilarity(true));
              Weight w = indexSearcher.createNormalizedWeight(q, true);
              LeafReaderContext ctx = (LeafReaderContext)indexSearcher.getTopReaderContext();
              Scorer scorer = w.scorer(ctx);
              if (scorer != null) {
                DocIdSetIterator iterator = scorer.iterator();
                boolean more = false;
                final Bits liveDocs = context.reader().getLiveDocs();
                for (int d = iterator.advance(lastDoc[0] + 1); d != DocIdSetIterator.NO_MORE_DOCS; d = iterator.nextDoc()) {
                  if (liveDocs == null || liveDocs.get(d)) {
                    more = true;
                    break;
                  }
                }
                Assert.assertFalse("query's last doc was "+ lastDoc[0] +" but advance("+(lastDoc[0]+1)+") got to "+scorer.docID(),more);
              }
              leafPtr++;
            }
            lastReader[0] = context.reader();
            assert readerContextArray.get(leafPtr).reader() == context.reader();
            this.scorer = null;
            lastDoc[0] = -1;
          }
        });

        if (lastReader[0] != null) {
          // confirm that skipping beyond the last doc, on the
          // previous reader, hits NO_MORE_DOCS
          final LeafReader previousReader = lastReader[0];
          IndexSearcher indexSearcher = LuceneTestCase.newSearcher(previousReader, false);
          indexSearcher.setSimilarity(s.getSimilarity(true));
          Weight w = indexSearcher.createNormalizedWeight(q, true);
          LeafReaderContext ctx = previousReader.getContext();
          Scorer scorer = w.scorer(ctx);
          if (scorer != null) {
            DocIdSetIterator iterator = scorer.iterator();
            boolean more = false;
            final Bits liveDocs = lastReader[0].getLiveDocs();
            for (int d = iterator.advance(lastDoc[0] + 1); d != DocIdSetIterator.NO_MORE_DOCS; d = iterator.nextDoc()) {
              if (liveDocs == null || liveDocs.get(d)) {
                more = true;
                break;
              }
            }
            Assert.assertFalse("query's last doc was "+ lastDoc[0] +" but advance("+(lastDoc[0]+1)+") got to "+scorer.docID(),more);
          }
        }
      }
  }

  /** check that first skip on just created scorers always goes to the right doc */
  public static void checkFirstSkipTo(final Query q, final IndexSearcher s) throws IOException {
    //System.out.println("checkFirstSkipTo: "+q);
    final float maxDiff = 1e-3f;
    final int lastDoc[] = {-1};
    final LeafReader lastReader[] = {null};
    final List<LeafReaderContext> context = s.getTopReaderContext().leaves();
    s.search(q,new SimpleCollector() {
      private Scorer scorer;
      private int leafPtr;
      @Override
      public void setScorer(Scorer scorer) {
        this.scorer = scorer;
      }
      @Override
      public void collect(int doc) throws IOException {
        float score = scorer.score();
        try {
          long startMS = System.currentTimeMillis();
          for (int i=lastDoc[0]+1; i<=doc; i++) {
            Weight w = s.createNormalizedWeight(q, true);
            Scorer scorer = w.scorer(context.get(leafPtr));
            Assert.assertTrue("query collected "+doc+" but advance("+i+") says no more docs!",scorer.iterator().advance(i) != DocIdSetIterator.NO_MORE_DOCS);
            Assert.assertEquals("query collected "+doc+" but advance("+i+") got to "+scorer.docID(),doc,scorer.docID());
            float advanceScore = scorer.score();
            Assert.assertEquals("unstable advance("+i+") score!",advanceScore,scorer.score(),maxDiff);
            Assert.assertEquals("query assigned doc "+doc+" a score of <"+score+"> but advance("+i+") has <"+advanceScore+">!",score,advanceScore,maxDiff);

            // Hurry things along if they are going slow (eg
            // if you got SimpleText codec this will kick in):
            if (i < doc && System.currentTimeMillis() - startMS > 5) {
              i = doc-1;
            }
          }
          lastDoc[0] = doc;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public boolean needsScores() {
        return true;
      }

      @Override
      protected void doSetNextReader(LeafReaderContext context) throws IOException {
        // confirm that skipping beyond the last doc, on the
        // previous reader, hits NO_MORE_DOCS
        if (lastReader[0] != null) {
          final LeafReader previousReader = lastReader[0];
          IndexSearcher indexSearcher = LuceneTestCase.newSearcher(previousReader, false);
          indexSearcher.setSimilarity(s.getSimilarity(true));
          Weight w = indexSearcher.createNormalizedWeight(q, true);
          Scorer scorer = w.scorer((LeafReaderContext)indexSearcher.getTopReaderContext());
          if (scorer != null) {
            DocIdSetIterator iterator = scorer.iterator();
            boolean more = false;
            final Bits liveDocs = context.reader().getLiveDocs();
            for (int d = iterator.advance(lastDoc[0] + 1); d != DocIdSetIterator.NO_MORE_DOCS; d = iterator.nextDoc()) {
              if (liveDocs == null || liveDocs.get(d)) {
                more = true;
                break;
              }
            }
            Assert.assertFalse("query's last doc was "+ lastDoc[0] +" but advance("+(lastDoc[0]+1)+") got to "+scorer.docID(),more);
          }
          leafPtr++;
        }

        lastReader[0] = context.reader();
        lastDoc[0] = -1;
      }
    });

    if (lastReader[0] != null) {
      // confirm that skipping beyond the last doc, on the
      // previous reader, hits NO_MORE_DOCS
      final LeafReader previousReader = lastReader[0];
      IndexSearcher indexSearcher = LuceneTestCase.newSearcher(previousReader, false);
      indexSearcher.setSimilarity(s.getSimilarity(true));
      Weight w = indexSearcher.createNormalizedWeight(q, true);
      Scorer scorer = w.scorer((LeafReaderContext)indexSearcher.getTopReaderContext());
      if (scorer != null) {
        DocIdSetIterator iterator = scorer.iterator();
        boolean more = false;
        final Bits liveDocs = lastReader[0].getLiveDocs();
        for (int d = iterator.advance(lastDoc[0] + 1); d != DocIdSetIterator.NO_MORE_DOCS; d = iterator.nextDoc()) {
          if (liveDocs == null || liveDocs.get(d)) {
            more = true;
            break;
          }
        }
        Assert.assertFalse("query's last doc was "+ lastDoc[0] +" but advance("+(lastDoc[0]+1)+") got to "+scorer.docID(),more);
      }
    }
  }

  /** Check that the scorer and bulk scorer advance consistently. */
  public static void checkBulkScorerSkipTo(Random r, Query query, IndexSearcher searcher) throws IOException {
    Weight weight = searcher.createNormalizedWeight(query, true);
    for (LeafReaderContext context : searcher.getIndexReader().leaves()) {
      final Scorer scorer = weight.scorer(context);
      final BulkScorer bulkScorer = weight.bulkScorer(context);
      if (scorer == null && bulkScorer == null) {
        continue;
      } else if (bulkScorer == null) {
        // ensure scorer is exhausted (it just didnt return null)
        assert scorer.iterator().nextDoc() == DocIdSetIterator.NO_MORE_DOCS;
        continue;
      }
      DocIdSetIterator iterator = scorer.iterator();
      int upTo = 0;
      while (true) {
        final int min = upTo + r.nextInt(5);
        final int max = min + 1 + r.nextInt(r.nextBoolean() ? 10 : 5000);
        if (scorer.docID() < min) {
          iterator.advance(min);
        }
        final int next = bulkScorer.score(new LeafCollector() {
          Scorer scorer2;
          @Override
          public void setScorer(Scorer scorer) throws IOException {
            this.scorer2 = scorer;
          }
          @Override
          public void collect(int doc) throws IOException {
            assert doc >= min;
            assert doc < max;
            Assert.assertEquals(scorer.docID(), doc);
            Assert.assertEquals(scorer.score(), scorer2.score(), 0.01f);
            iterator.nextDoc();
          }
        }, null, min, max);
        assert max <= next;
        assert next <= scorer.docID();
        upTo = max;

        if (scorer.docID() == DocIdSetIterator.NO_MORE_DOCS) {
          bulkScorer.score(new LeafCollector() {
            @Override
            public void setScorer(Scorer scorer) throws IOException {}

            @Override
            public void collect(int doc) throws IOException {
              // no more matches
              assert false;
            }
          }, null, upTo, DocIdSetIterator.NO_MORE_DOCS);
          break;
        }
      }
    }
  }
}
