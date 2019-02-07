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
import org.apache.lucene.document.Field.Store;
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
    QueryUtils.checkEqual(new SynonymQuery(), new SynonymQuery());
    QueryUtils.checkEqual(new SynonymQuery(new Term("foo", "bar")),
                          new SynonymQuery(new Term("foo", "bar")));

    QueryUtils.checkEqual(new SynonymQuery(new Term("a", "a"), new Term("a", "b")),
                          new SynonymQuery(new Term("a", "b"), new Term("a", "a")));
  }

  public void testBogusParams() {
    expectThrows(IllegalArgumentException.class, () -> {
      new SynonymQuery(new Term("field1", "a"), new Term("field2", "b"));
    });
  }

  public void testToString() {
    assertEquals("Synonym()", new SynonymQuery().toString());
    Term t1 = new Term("foo", "bar");
    assertEquals("Synonym(foo:bar)", new SynonymQuery(t1).toString());
    Term t2 = new Term("foo", "baz");
    assertEquals("Synonym(foo:bar foo:baz)", new SynonymQuery(t1, t2).toString());
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

    IndexReader reader = w.getReader();
    IndexSearcher searcher = newSearcher(reader);
    SynonymQuery query = new SynonymQuery(new Term("f", "a"), new Term("f", "b"));

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

    ImpactsSource mergedImpacts = SynonymQuery.mergeImpacts(new ImpactsEnum[] { impacts1, impacts2 });
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
    int numDocs = atLeast(128 * 8 * 8 * 3); // make sure some terms have skip data
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
      Query query = new SynonymQuery(
          new Term[] {
              new Term("foo", Integer.toString(term1)),
              new Term("foo", Integer.toString(term2))});

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
