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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestSubScorerFreqs extends LuceneTestCase {

  private static Directory dir;
  private static IndexSearcher s;

  @BeforeClass
  public static void makeIndex() throws Exception {
    dir = new RAMDirectory();
    RandomIndexWriter w = new RandomIndexWriter(
        random(), dir, newIndexWriterConfig(new MockAnalyzer(random())).setMergePolicy(newLogMergePolicy()));
    // make sure we have more than one segment occationally
    int num = atLeast(31);
    for (int i = 0; i < num; i++) {
      Document doc = new Document();
      doc.add(newTextField("f", "a b c d b c d c d d", Field.Store.NO));
      w.addDocument(doc);

      doc = new Document();
      doc.add(newTextField("f", "a b c d", Field.Store.NO));
      w.addDocument(doc);
    }

    s = newSearcher(w.getReader());
    s.setSimilarity(new CountingSimilarity());
    w.close();
  }

  @AfterClass
  public static void finish() throws Exception {
    s.getIndexReader().close();
    s = null;
    dir.close();
    dir = null;
  }

  private static class CountingCollector extends FilterCollector {
    public final Map<Integer, Map<Query, Float>> docCounts = new HashMap<>();

    private final Map<Query, Scorable> subScorers = new HashMap<>();
    private final Set<String> relationships;

    public CountingCollector(Collector other) {
      this(other, new HashSet<>(Arrays.asList("MUST", "SHOULD", "MUST_NOT")));
    }

    public CountingCollector(Collector other, Set<String> relationships) {
      super(other);
      this.relationships = relationships;
    }
    
    public void setSubScorers(Scorable scorer) throws IOException {
      scorer = AssertingScorable.unwrap(scorer);
      for (Scorable.ChildScorable child : scorer.getChildren()) {
        if (relationships.contains(child.relationship)) {
          setSubScorers(child.child);
        }
      }
      subScorers.put(((Scorer)scorer).getWeight().getQuery(), scorer);
    }
    
    public LeafCollector getLeafCollector(LeafReaderContext context)
        throws IOException {
      final int docBase = context.docBase;
      return new FilterLeafCollector(super.getLeafCollector(context)) {
        
        @Override
        public void collect(int doc) throws IOException {
          final Map<Query, Float> freqs = new HashMap<Query, Float>();
          for (Map.Entry<Query, Scorable> ent : subScorers.entrySet()) {
            Scorable value = ent.getValue();
            int matchId = value.docID();
            freqs.put(ent.getKey(), matchId == doc ? value.score() : 0.0f);
          }
          docCounts.put(doc + docBase, freqs);
          super.collect(doc);
        }
        
        @Override
        public void setScorer(Scorable scorer) throws IOException {
          super.setScorer(scorer);
          subScorers.clear();
          setSubScorers(scorer);
        }
        
      };
    }

  }

  private static final float FLOAT_TOLERANCE = 0.00001F;

  @Test
  public void testTermQuery() throws Exception {
    TermQuery q = new TermQuery(new Term("f", "d"));
    CountingCollector c = new CountingCollector(TopScoreDocCollector.create(10, Integer.MAX_VALUE));
    s.search(q, c);
    final int maxDocs = s.getIndexReader().maxDoc();
    assertEquals(maxDocs, c.docCounts.size());
    for (int i = 0; i < maxDocs; i++) {
      Map<Query, Float> doc0 = c.docCounts.get(i);
      assertEquals(1, doc0.size());
      assertEquals(4.0F, doc0.get(q), FLOAT_TOLERANCE);

      Map<Query, Float> doc1 = c.docCounts.get(++i);
      assertEquals(1, doc1.size());
      assertEquals(1.0F, doc1.get(q), FLOAT_TOLERANCE);
    }
  }

  @Test
  public void testBooleanQuery() throws Exception {
    TermQuery aQuery = new TermQuery(new Term("f", "a"));
    TermQuery dQuery = new TermQuery(new Term("f", "d"));
    TermQuery cQuery = new TermQuery(new Term("f", "c"));
    TermQuery yQuery = new TermQuery(new Term("f", "y"));

    BooleanQuery.Builder query = new BooleanQuery.Builder();
    BooleanQuery.Builder inner = new BooleanQuery.Builder();

    inner.add(cQuery, Occur.SHOULD);
    inner.add(yQuery, Occur.MUST_NOT);
    query.add(inner.build(), Occur.MUST);
    query.add(aQuery, Occur.MUST);
    query.add(dQuery, Occur.MUST);
    
    // Only needed in Java6; Java7+ has a @SafeVarargs annotated Arrays#asList()!
    // see http://docs.oracle.com/javase/7/docs/api/java/lang/SafeVarargs.html
    @SuppressWarnings("unchecked") final Iterable<Set<String>> occurList = Arrays.asList(
        Collections.singleton("MUST"), 
        new HashSet<>(Arrays.asList("MUST", "SHOULD"))
    );
    
    for (final Set<String> occur : occurList) {
      CountingCollector c = new CountingCollector(TopScoreDocCollector.create(
          10, Integer.MAX_VALUE), occur);
      s.search(query.build(), c);
      final int maxDocs = s.getIndexReader().maxDoc();
      assertEquals(maxDocs, c.docCounts.size());
      boolean includeOptional = occur.contains("SHOULD");
      for (int i = 0; i < maxDocs; i++) {
        Map<Query, Float> doc0 = c.docCounts.get(i);
        // Y doesnt exist in the index, so it's not in the scorer tree
        assertEquals(4, doc0.size());
        assertEquals(1.0F, doc0.get(aQuery), FLOAT_TOLERANCE);
        assertEquals(4.0F, doc0.get(dQuery), FLOAT_TOLERANCE);
        if (includeOptional) {
          assertEquals(3.0F, doc0.get(cQuery), FLOAT_TOLERANCE);
        }

        Map<Query, Float> doc1 = c.docCounts.get(++i);
        // Y doesnt exist in the index, so it's not in the scorer tree
        assertEquals(4, doc1.size());
        assertEquals(1.0F, doc1.get(aQuery), FLOAT_TOLERANCE);
        assertEquals(1.0F, doc1.get(dQuery), FLOAT_TOLERANCE);
        if (includeOptional) {
          assertEquals(1.0F, doc1.get(cQuery), FLOAT_TOLERANCE);
        }
      }
    }
  }

  @Test
  public void testPhraseQuery() throws Exception {
    PhraseQuery q = new PhraseQuery("f", "b", "c");
    CountingCollector c = new CountingCollector(TopScoreDocCollector.create(10, Integer.MAX_VALUE));
    s.search(q, c);
    final int maxDocs = s.getIndexReader().maxDoc();
    assertEquals(maxDocs, c.docCounts.size());
    for (int i = 0; i < maxDocs; i++) {
      Map<Query, Float> doc0 = c.docCounts.get(i);
      assertEquals(1, doc0.size());
      assertEquals(2.0F, doc0.get(q), FLOAT_TOLERANCE);

      Map<Query, Float> doc1 = c.docCounts.get(++i);
      assertEquals(1, doc1.size());
      assertEquals(1.0F, doc1.get(q), FLOAT_TOLERANCE);
    }

  }

  // Similarity that just returns the frequency as the score
  private static class CountingSimilarity extends Similarity {

    @Override
    public long computeNorm(FieldInvertState state) {
      return 1;
    }

    @Override
    public SimScorer scorer(float boost, CollectionStatistics collectionStats, TermStatistics... termStats) {
      return new SimScorer() {
        @Override
        public float score(float freq, long norm) {
          return freq;
        }
      };
    }
  }
}
