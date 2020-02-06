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
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Impact;
import org.apache.lucene.index.Impacts;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.ImpactsSource;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestSynonymQuery extends LuceneTestCase {

  public void testEquals() {
    QueryUtils.checkEqual(new SynonymQuery.Builder("foo").build(), new SynonymQuery.Builder("foo").build());
    QueryUtils.checkEqual(new SynonymQuery.Builder("foo").addTerm(new Term("foo", "bar")).build(),
                          new SynonymQuery.Builder("foo").addTerm(new Term("foo", "bar")).build());

    QueryUtils.checkEqual(new SynonymQuery.Builder("a").addTerm(new Term("a", "a")).addTerm(new Term("a", "b")).build(),
                          new SynonymQuery.Builder("a").addTerm(new Term("a", "b")).addTerm(new Term("a", "a")).build());

    QueryUtils.checkEqual(
        new SynonymQuery.Builder("field")
            .addTerm(new Term("field", "b"), 0.4f)
            .addTerm(new Term("field", "c"), 0.2f)
            .addTerm(new Term("field", "d")).build(),
        new SynonymQuery.Builder("field")
            .addTerm(new Term("field", "b"), 0.4f)
            .addTerm(new Term("field", "c"), 0.2f)
            .addTerm(new Term("field", "d")).build());

  }

  public void testBogusParams() {
    expectThrows(IllegalArgumentException.class, () -> {
      new SynonymQuery.Builder("field1").addTerm(new Term("field1", "a")).addTerm(new Term("field2", "b"));
    });

    expectThrows(IllegalArgumentException.class, () -> {
      new SynonymQuery.Builder("field1").addTerm(new Term("field1", "a"), 1.3f);
    });

    expectThrows(IllegalArgumentException.class, () -> {
      new SynonymQuery.Builder("field1").addTerm(new Term("field1", "a"), Float.NaN);
    });

    expectThrows(IllegalArgumentException.class, () -> {
      new SynonymQuery.Builder("field1").addTerm(new Term("field1", "a"), Float.POSITIVE_INFINITY);
    });

    expectThrows(IllegalArgumentException.class, () -> {
      new SynonymQuery.Builder("field1").addTerm(new Term("field1", "a"), Float.NEGATIVE_INFINITY);
    });

    expectThrows(IllegalArgumentException.class, () -> {
      new SynonymQuery.Builder("field1").addTerm(new Term("field1", "a"), -0.3f);
    });

    expectThrows(IllegalArgumentException.class, () -> {
      new SynonymQuery.Builder("field1").addTerm(new Term("field1", "a"), 0f);
    });

    expectThrows(IllegalArgumentException.class, () -> {
      new SynonymQuery.Builder("field1").addTerm(new Term("field1", "a"), -0f);
    });
  }

  public void testToString() {
    assertEquals("Synonym()", new SynonymQuery.Builder("foo").build().toString());
    Term t1 = new Term("foo", "bar");
    assertEquals("Synonym(foo:bar)", new SynonymQuery.Builder("foo")
        .addTerm(t1).build().toString());
    Term t2 = new Term("foo", "baz");
    assertEquals("Synonym(foo:bar foo:baz)", new SynonymQuery.Builder("foo")
        .addTerm(t1).addTerm(t2).build().toString());
  }

  public void testScores() throws IOException {
    doTestScores(2);
    doTestScores(Integer.MAX_VALUE);
  }

  private void doTestScores(int totalHitsThreshold) throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    Document doc = new Document();
    doc.add(new StringField("f", "a", Store.NO));
    w.addDocument(doc);

    doc = new Document();
    doc.add(new StringField("f", "b", Store.NO));
    for (int i = 0; i < 10; ++i) {
      w.addDocument(doc);
    }
    float boost = random().nextBoolean() ? random().nextFloat() : 1f;
    IndexReader reader = w.getReader();
    IndexSearcher searcher = newSearcher(reader);
    SynonymQuery query = new SynonymQuery.Builder("f")
        .addTerm(new Term("f", "a"), boost == 0 ? 1f : boost)
        .addTerm(new Term("f", "b"), boost == 0 ? 1f : boost)
        .build();

    TopScoreDocCollector collector = TopScoreDocCollector.create(Math.min(reader.numDocs(), totalHitsThreshold), null, totalHitsThreshold);
    searcher.search(query, collector);
    TopDocs topDocs = collector.topDocs();
    if (topDocs.totalHits.value < totalHitsThreshold) {
      assertEquals(new TotalHits(11, TotalHits.Relation.EQUAL_TO), topDocs.totalHits);
    } else {
      assertEquals(TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO, topDocs.totalHits.relation);
    }
    // All docs must have the same score
    for (int i = 0; i < topDocs.scoreDocs.length; ++i) {
      assertEquals(topDocs.scoreDocs[0].score, topDocs.scoreDocs[i].score, 0.0f);
    }

    reader.close();
    w.close();
    dir.close();
  }

  public void testBoosts() throws IOException {
    doTestBoosts(2);
    doTestBoosts(Integer.MAX_VALUE);
  }

  public void doTestBoosts(int totalHitsThreshold) throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    Document doc = new Document();
    FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
    ft.setOmitNorms(true);
    doc.add(new Field("f", "c", ft));
    w.addDocument(doc);
    for (int i = 0; i < 10; ++i) {
      doc.clear();
      doc.add(new Field("f", "a a a a", ft));
      w.addDocument(doc);
      if (i % 2 == 0) {
        doc.clear();
        doc.add(new Field("f", "b b", ft));
        w.addDocument(doc);
      } else {
        doc.clear();
        doc.add(new Field("f", "a a b", ft));
        w.addDocument(doc);
      }
    }
    doc.clear();
    doc.add(new Field("f", "c", ft));
    w.addDocument(doc);
    IndexReader reader = w.getReader();
    IndexSearcher searcher = newSearcher(reader);
    SynonymQuery query = new SynonymQuery.Builder("f")
        .addTerm(new Term("f", "a"), 0.25f)
        .addTerm(new Term("f", "b"), 0.5f)
        .addTerm(new Term("f", "c"))
        .build();

    TopScoreDocCollector collector = TopScoreDocCollector.create(Math.min(reader.numDocs(), totalHitsThreshold), null, totalHitsThreshold);
    searcher.search(query, collector);
    TopDocs topDocs = collector.topDocs();
    if (topDocs.totalHits.value < totalHitsThreshold) {
      assertEquals(TotalHits.Relation.EQUAL_TO, topDocs.totalHits.relation);
      assertEquals(22, topDocs.totalHits.value);
    } else {
      assertEquals(TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO, topDocs.totalHits.relation);
    }
    // All docs must have the same score
    for (int i = 0; i < topDocs.scoreDocs.length; ++i) {
      assertEquals(topDocs.scoreDocs[0].score, topDocs.scoreDocs[i].score, 0.0f);
    }

    reader.close();
    w.close();
    dir.close();
  }

  public void testMergeImpacts() throws IOException {
    DummyImpactsEnum impacts1 = new DummyImpactsEnum();
    impacts1.reset(42,
        new Impact[][] {
          new Impact[] { new Impact(3, 10), new Impact(5, 12), new Impact(8, 13) },
          new Impact[] { new Impact(5, 11), new Impact(8, 13),  new Impact(12, 14) }
        },
        new int[] {
            110,
            945
        });
    DummyImpactsEnum impacts2 = new DummyImpactsEnum();
    impacts2.reset(45,
        new Impact[][] {
          new Impact[] { new Impact(2, 10), new Impact(6, 13) },
          new Impact[] { new Impact(3, 9), new Impact(5, 11), new Impact(7, 13) }
        },
        new int[] {
            90,
            1000
        });

    ImpactsSource mergedImpacts = SynonymQuery.mergeImpacts(new ImpactsEnum[] { impacts1, impacts2 }, new float[] { 1f, 1f });
    assertEquals(
        new Impact[][] {
          new Impact[] { new Impact(5, 10), new Impact(7, 12), new Impact(14, 13) },
          new Impact[] { new Impact(Integer.MAX_VALUE, 1) }
        },
        new int[] {
            90,
            1000
        },
        mergedImpacts.getImpacts());

    ImpactsSource mergedBoostedImpacts = SynonymQuery.mergeImpacts(new ImpactsEnum[] { impacts1, impacts2 }, new float[] { 0.3f, 0.9f });
    assertEquals(
        new Impact[][] {
            new Impact[] { new Impact(3, 10), new Impact(4, 12), new Impact(9, 13) },
            new Impact[] { new Impact(Integer.MAX_VALUE, 1) }
        },
        new int[] {
            90,
            1000
        },
        mergedBoostedImpacts.getImpacts());

    // docID is > the first doIdUpTo of impacts1
    impacts2.reset(112,
        new Impact[][] {
          new Impact[] { new Impact(2, 10), new Impact(6, 13) },
          new Impact[] { new Impact(3, 9), new Impact(5, 11), new Impact(7, 13) }
        },
        new int[] {
            150,
            1000
        });
    assertEquals(
        new Impact[][] {
          new Impact[] { new Impact(3, 10), new Impact(5, 12), new Impact(8, 13) }, // same as impacts1
          new Impact[] { new Impact(3, 9), new Impact(10, 11), new Impact(15, 13), new Impact(19, 14) }
        },
        new int[] {
            110,
            945
        },
        mergedImpacts.getImpacts());

    assertEquals(
        new Impact[][] {
            new Impact[] { new Impact(1, 10), new Impact(2, 12), new Impact(3, 13) }, // same as impacts1*boost
            new Impact[] { new Impact(3, 9), new Impact(7, 11), new Impact(10, 13), new Impact(11, 14) }
        },
        new int[] {
            110,
            945
        },
        mergedBoostedImpacts.getImpacts());
  }

  private static void assertEquals(Impact[][] impacts, int[] docIdUpTo, Impacts actual) {
    assertEquals(impacts.length, actual.numLevels());
    for (int i = 0; i < impacts.length; ++i) {
      assertEquals(docIdUpTo[i], actual.getDocIdUpTo(i));
      assertEquals(Arrays.asList(impacts[i]), actual.getImpacts(i));
    }
  }

  private static class DummyImpactsEnum extends ImpactsEnum {

    private int docID;
    private Impact[][] impacts;
    private int[] docIdUpTo;

    void reset(int docID, Impact[][] impacts, int[] docIdUpTo) {
      this.docID = docID;
      this.impacts = impacts;
      this.docIdUpTo = docIdUpTo;
    }

    @Override
    public void advanceShallow(int target) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Impacts getImpacts() throws IOException {
      return new Impacts() {

        @Override
        public int numLevels() {
          return impacts.length;
        }

        @Override
        public int getDocIdUpTo(int level) {
          return docIdUpTo[level];
        }

        @Override
        public List<Impact> getImpacts(int level) {
          return Arrays.asList(impacts[level]);
        }

      };
    }

    @Override
    public int freq() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int nextPosition() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int startOffset() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int endOffset() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public BytesRef getPayload() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int docID() {
      return docID;
    }

    @Override
    public int nextDoc() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int advance(int target) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public long cost() {
      throw new UnsupportedOperationException();
    }

  }

  public void testRandomTopDocs() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    int numDocs = TEST_NIGHTLY ? atLeast(128 * 8 * 8 * 3) : atLeast(100); // at night, make sure some terms have skip data
    for (int i = 0; i < numDocs; ++i) {
      Document doc = new Document();
      int numValues = random().nextInt(1 << random().nextInt(5));
      int start = random().nextInt(10);
      for (int j = 0; j < numValues; ++j) {
        int freq = TestUtil.nextInt(random(), 1, 1 << random().nextInt(3));
        for (int k = 0; k < freq; ++k) {
          doc.add(new TextField("foo", Integer.toString(start + j), Store.NO));
        }
      }
      w.addDocument(doc);
    }
    IndexReader reader = DirectoryReader.open(w);
    w.close();
    IndexSearcher searcher = newSearcher(reader);

    for (int term1 = 0; term1 < 15; ++term1) {
      int term2;
      do {
        term2 = random().nextInt(15);
      } while (term1 == term2);
      float boost1 = random().nextBoolean() ? Math.max(random().nextFloat(), Float.MIN_NORMAL) : 1f;
      float boost2 = random().nextBoolean() ? Math.max(random().nextFloat(), Float.MIN_NORMAL) : 1f;
      Query query = new SynonymQuery.Builder("foo")
          .addTerm(new Term("foo", Integer.toString(term1)), boost1)
          .addTerm(new Term("foo", Integer.toString(term2)), boost2)
          .build();

      TopScoreDocCollector collector1 = TopScoreDocCollector.create(10, null, Integer.MAX_VALUE); // COMPLETE
      TopScoreDocCollector collector2 = TopScoreDocCollector.create(10, null, 1); // TOP_SCORES

      searcher.search(query, collector1);
      searcher.search(query, collector2);
      CheckHits.checkEqual(query, collector1.topDocs().scoreDocs, collector2.topDocs().scoreDocs);

      int filterTerm = random().nextInt(15);
      Query filteredQuery = new BooleanQuery.Builder()
          .add(query, Occur.MUST)
          .add(new TermQuery(new Term("foo", Integer.toString(filterTerm))), Occur.FILTER)
          .build();

      collector1 = TopScoreDocCollector.create(10, null, Integer.MAX_VALUE); // COMPLETE
      collector2 = TopScoreDocCollector.create(10, null, 1); // TOP_SCORES
      searcher.search(filteredQuery, collector1);
      searcher.search(filteredQuery, collector2);
      CheckHits.checkEqual(query, collector1.topDocs().scoreDocs, collector2.topDocs().scoreDocs);
    }
    reader.close();
    dir.close();
  }
}
