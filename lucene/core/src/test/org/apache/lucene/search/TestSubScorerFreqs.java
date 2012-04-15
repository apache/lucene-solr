package org.apache.lucene.search;

/**
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
        random(), dir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())).setMergePolicy(newLogMergePolicy()));
    // make sure we have more than one segment occationally
    int num = atLeast(31);
    for (int i = 0; i < num; i++) {
      Document doc = new Document();
      doc.add(newField("f", "a b c d b c d c d d", TextField.TYPE_UNSTORED));
      w.addDocument(doc);

      doc = new Document();
      doc.add(newField("f", "a b c d", TextField.TYPE_UNSTORED));
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

  private static class CountingCollector extends Collector {
    private final Collector other;
    private int docBase;

    public final Map<Integer, Map<Query, Float>> docCounts = new HashMap<Integer, Map<Query, Float>>();

    private final Map<Query, Scorer> subScorers = new HashMap<Query, Scorer>();
    private final Set<String> relationships;

    public CountingCollector(Collector other) {
      this(other, new HashSet<String>(Arrays.asList(Occur.MUST.toString(), Occur.SHOULD.toString(), Occur.MUST_NOT.toString())));
    }

    public CountingCollector(Collector other, Set<String> relationships) {
      this.other = other;
      this.relationships = relationships;
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
      other.setScorer(scorer);
      subScorers.clear();
      setSubScorers(scorer, "TOP");
    }
    
    public void setSubScorers(Scorer scorer, String relationship) {
      for (ChildScorer child : scorer.getChildren()) {
        if (relationships.contains(child.relationship)) {
          setSubScorers(child.child, child.relationship);
        }
      }
      subScorers.put(scorer.getWeight().getQuery(), scorer);
    }

    @Override
    public void collect(int doc) throws IOException {
      final Map<Query, Float> freqs = new HashMap<Query, Float>();
      for (Map.Entry<Query, Scorer> ent : subScorers.entrySet()) {
        Scorer value = ent.getValue();
        int matchId = value.docID();
        freqs.put(ent.getKey(), matchId == doc ? value.freq() : 0.0f);
      }
      docCounts.put(doc + docBase, freqs);
      other.collect(doc);
    }

    @Override
    public void setNextReader(AtomicReaderContext context)
        throws IOException {
      docBase = context.docBase;
      other.setNextReader(context);
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
      return other.acceptsDocsOutOfOrder();
    }
  }

  private static final float FLOAT_TOLERANCE = 0.00001F;

  @Test
  public void testTermQuery() throws Exception {
    TermQuery q = new TermQuery(new Term("f", "d"));
    CountingCollector c = new CountingCollector(TopScoreDocCollector.create(10,
        true));
    s.search(q, null, c);
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

  @SuppressWarnings("unchecked")
  @Test
  public void testBooleanQuery() throws Exception {
    TermQuery aQuery = new TermQuery(new Term("f", "a"));
    TermQuery dQuery = new TermQuery(new Term("f", "d"));
    TermQuery cQuery = new TermQuery(new Term("f", "c"));
    TermQuery yQuery = new TermQuery(new Term("f", "y"));

    BooleanQuery query = new BooleanQuery();
    BooleanQuery inner = new BooleanQuery();

    inner.add(cQuery, Occur.SHOULD);
    inner.add(yQuery, Occur.MUST_NOT);
    query.add(inner, Occur.MUST);
    query.add(aQuery, Occur.MUST);
    query.add(dQuery, Occur.MUST);
    @SuppressWarnings({"rawtypes","unchecked"}) Set<String>[] occurList = new Set[] {
        Collections.singleton(Occur.MUST.toString()), 
        new HashSet<String>(Arrays.asList(Occur.MUST.toString(), Occur.SHOULD.toString()))
    };
    for (Set<String> occur : occurList) {
      CountingCollector c = new CountingCollector(TopScoreDocCollector.create(
          10, true), occur);
      s.search(query, null, c);
      final int maxDocs = s.getIndexReader().maxDoc();
      assertEquals(maxDocs, c.docCounts.size());
      boolean includeOptional = occur.contains(Occur.SHOULD.toString());
      for (int i = 0; i < maxDocs; i++) {
        Map<Query, Float> doc0 = c.docCounts.get(i);
        assertEquals(includeOptional ? 5 : 4, doc0.size());
        assertEquals(1.0F, doc0.get(aQuery), FLOAT_TOLERANCE);
        assertEquals(4.0F, doc0.get(dQuery), FLOAT_TOLERANCE);
        if (includeOptional)
          assertEquals(3.0F, doc0.get(cQuery), FLOAT_TOLERANCE);

        Map<Query, Float> doc1 = c.docCounts.get(++i);
        assertEquals(includeOptional ? 5 : 4, doc1.size());
        assertEquals(1.0F, doc1.get(aQuery), FLOAT_TOLERANCE);
        assertEquals(1.0F, doc1.get(dQuery), FLOAT_TOLERANCE);
        if (includeOptional)
          assertEquals(1.0F, doc1.get(cQuery), FLOAT_TOLERANCE);

      }
    }
  }

  @Test
  public void testPhraseQuery() throws Exception {
    PhraseQuery q = new PhraseQuery();
    q.add(new Term("f", "b"));
    q.add(new Term("f", "c"));
    CountingCollector c = new CountingCollector(TopScoreDocCollector.create(10,
        true));
    s.search(q, null, c);
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
