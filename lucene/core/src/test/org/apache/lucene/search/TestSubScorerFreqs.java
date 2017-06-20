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


import java.io.*;
import java.util.*;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.Scorer.ChildScorer;
import org.apache.lucene.store.*;
import org.apache.lucene.util.*;
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

    private final Map<Query, Scorer> subScorers = new HashMap<>();
    private final Set<String> relationships;

    public CountingCollector(Collector other) {
      this(other, new HashSet<>(Arrays.asList("MUST", "SHOULD", "MUST_NOT")));
    }

    public CountingCollector(Collector other, Set<String> relationships) {
      super(other);
      this.relationships = relationships;
    }
    
    public void setSubScorers(Scorer scorer, String relationship) throws IOException {
      for (ChildScorer child : scorer.getChildren()) {
        if (scorer instanceof AssertingScorer || relationships.contains(child.relationship)) {
          setSubScorers(child.child, child.relationship);
        }
      }
      subScorers.put(scorer.getWeight().getQuery(), scorer);
    }
    
    public LeafCollector getLeafCollector(LeafReaderContext context)
        throws IOException {
      final int docBase = context.docBase;
      return new FilterLeafCollector(super.getLeafCollector(context)) {
        
        @Override
        public void collect(int doc) throws IOException {
          final Map<Query, Float> freqs = new HashMap<Query, Float>();
          for (Map.Entry<Query, Scorer> ent : subScorers.entrySet()) {
            Scorer value = ent.getValue();
            int matchId = value.docID();
            freqs.put(ent.getKey(), matchId == doc ? value.freq() : 0.0f);
          }
          docCounts.put(doc + docBase, freqs);
          super.collect(doc);
        }
        
        @Override
        public void setScorer(Scorer scorer) throws IOException {
          super.setScorer(scorer);
          subScorers.clear();
          setSubScorers(scorer, "TOP");
        }
        
      };
    }

  }

  private static final float FLOAT_TOLERANCE = 0.00001F;

  @Test
  public void testTermQuery() throws Exception {
    TermQuery q = new TermQuery(new Term("f", "d"));
    CountingCollector c = new CountingCollector(TopScoreDocCollector.create(10));
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
          10), occur);
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
    CountingCollector c = new CountingCollector(TopScoreDocCollector.create(10));
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
}
