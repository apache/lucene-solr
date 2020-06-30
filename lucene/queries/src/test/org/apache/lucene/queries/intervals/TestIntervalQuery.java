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

package org.apache.lucene.queries.intervals;

import java.io.IOException;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.CheckHits;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

public class TestIntervalQuery extends LuceneTestCase {

  private IndexSearcher searcher;
  private IndexReader reader;
  private Directory directory;

  public static final String field = "field";

  @Override
  public void setUp() throws Exception {
    super.setUp();
    directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory, newIndexWriterConfig(new MockAnalyzer(random())).setMergePolicy(newLogMergePolicy()));
    for (int i = 0; i < docFields.length; i++) {
      Document doc = new Document();
      doc.add(newTextField(field, docFields[i], Field.Store.YES));
      writer.addDocument(doc);
    }
    reader = writer.getReader();
    writer.close();
    searcher = newSearcher(reader);
  }

  @Override
  public void tearDown() throws Exception {
    reader.close();
    directory.close();
    super.tearDown();
  }

  private String[] docFields = {
      "w1 w2 w3 w4 w5",
      "w1 w3 w2 w3",
      "w1 xx w2 w4 yy w3",
      "w1 w3 xx w2 yy w3",
      "w2 w1",
      "w2 w1 w3 w2 w4",
      "coordinate genome mapping research",
      "coordinate genome research",
      "greater new york",
      "x x x x x intend x x x message x x x message x x x addressed x x",
      "issue with intervals queries from search engine. So it's a big issue for us as we need to do ordered searches. Thank you to help us concerning that issue"
  };

  private void checkHits(Query query, int[] results) throws IOException {
    CheckHits.checkHits(random(), query, field, searcher, results);
  }

  public void testPhraseQuery() throws IOException {
    checkHits(new IntervalQuery(field, Intervals.phrase(Intervals.term("w1"), Intervals.term("w2"))),
        new int[]{0});
  }

  public void testOrderedNearQueryWidth3() throws IOException {
    checkHits(new IntervalQuery(field, Intervals.maxwidth(3, Intervals.ordered(Intervals.term("w1"), Intervals.term("w2")))),
        new int[]{0, 1, 2, 5});
  }

  public void testOrderedNearQueryWidth4() throws IOException {
    checkHits(new IntervalQuery(field, Intervals.maxwidth(4, Intervals.ordered(Intervals.term("w1"), Intervals.term("w2")))),
        new int[]{0, 1, 2, 3, 5});
  }

  public void testOrderedNearQueryGaps1() throws IOException {
    checkHits(new IntervalQuery(field, Intervals.maxgaps(1, Intervals.ordered(Intervals.term("w1"), Intervals.term("w2")))),
        new int[]{0, 1, 2, 5});
  }

  public void testOrderedNearQueryGaps2() throws IOException {
    checkHits(new IntervalQuery(field, Intervals.maxgaps(2, Intervals.ordered(Intervals.term("w1"), Intervals.term("w2")))),
        new int[]{0, 1, 2, 3, 5});
  }

  public void testNestedOrderedNearQuery() throws IOException {
    // onear/1(w1, onear/2(w2, w3))
    Query q = new IntervalQuery(field,
        Intervals.ordered(
            Intervals.term("w1"),
            Intervals.maxwidth(3, Intervals.ordered(Intervals.term("w2"), Intervals.term("w3")))));

    checkHits(q, new int[]{0, 1, 3});
  }

  public void testUnorderedQuery() throws IOException {
    Query q = new IntervalQuery(field, Intervals.unordered(Intervals.term("w1"), Intervals.term("w3")));
    checkHits(q, new int[]{0, 1, 2, 3, 5});
  }

  public void testNonOverlappingQuery() throws IOException {
    Query q = new IntervalQuery(field, Intervals.nonOverlapping(
        Intervals.unordered(Intervals.term("w1"), Intervals.term("w3")),
        Intervals.unordered(Intervals.term("w2"), Intervals.term("w4"))));
    checkHits(q, new int[]{1, 3, 5});
  }

  public void testFieldInToString() throws IOException {
    final IntervalQuery fieldW1 = new IntervalQuery(field, Intervals.term("w1"));
    assertTrue(fieldW1.toString().contains(field));
    final String field2 = field+"2";
    final IntervalQuery f2w1 = new IntervalQuery(field2, Intervals.term("w1"));
    assertTrue(f2w1.toString().contains(field2+":"));
    assertFalse("suppress default field",f2w1.toString(field2).contains(field2));
    
    final Explanation explain = searcher.explain(new IntervalQuery(field, 
        Intervals.ordered(Intervals.term("w1"), Intervals.term("w2"))), searcher.search(fieldW1, 1).scoreDocs[0].doc);
    assertTrue(explain.toString().contains(field));
  }
  
  public void testNullConstructorArgs() throws IOException {
    expectThrows(NullPointerException.class, ()-> new IntervalQuery(null, Intervals.term("z")));
    expectThrows(NullPointerException.class, ()-> new IntervalQuery("field", null));
  }
  
  
  public void testNotWithinQuery() throws IOException {
    Query q = new IntervalQuery(field, Intervals.notWithin(Intervals.term("w1"), 1, Intervals.term("w2")));
    checkHits(q, new int[]{ 1, 2, 3 });
  }

  public void testNotContainingQuery() throws IOException {
    Query q = new IntervalQuery(field, Intervals.notContaining(
        Intervals.unordered(Intervals.term("w1"), Intervals.term("w2")),
        Intervals.term("w3")
    ));
    checkHits(q, new int[]{ 0, 2, 4, 5 });
  }

  public void testContainingQuery() throws IOException {
    Query q = new IntervalQuery(field, Intervals.containing(
        Intervals.unordered(Intervals.term("w1"), Intervals.term("w2")),
        Intervals.term("w3")
    ));
    checkHits(q, new int[]{ 1, 3, 5 });
  }

  public void testContainedByQuery() throws IOException {
    Query q = new IntervalQuery(field, Intervals.containedBy(
        Intervals.term("w3"),
        Intervals.unordered(Intervals.term("w1"), Intervals.term("w2"))));
    checkHits(q, new int[]{ 1, 3, 5 });
  }

  public void testNotContainedByQuery() throws IOException {
    Query q = new IntervalQuery(field, Intervals.notContainedBy(
        Intervals.term("w2"),
        Intervals.unordered(Intervals.term("w1"), Intervals.term("w4"))
    ));
    checkHits(q, new int[]{ 1, 3, 4, 5 });
  }

  public void testNonExistentTerms() throws IOException {
    Query q = new IntervalQuery(field, Intervals.ordered(Intervals.term("w0"), Intervals.term("w2")));
    checkHits(q, new int[]{});
  }

  public void testNestedOr() throws IOException {
    Query q = new IntervalQuery(field, Intervals.phrase(
        Intervals.term("coordinate"),
        Intervals.or(Intervals.phrase("genome", "mapping"), Intervals.term("genome")),
        Intervals.term("research")));
    checkHits(q, new int[]{ 6, 7 });
  }

  public void testNestedOrWithGaps() throws IOException {
    Query q = new IntervalQuery(field, Intervals.phrase(
        Intervals.term("coordinate"),
        Intervals.or(Intervals.term("genome"), Intervals.extend(Intervals.term("mapping"), 1, 0)),
        Intervals.term("research")));
    checkHits(q, new int[]{ 6, 7 });
  }

  public void testNestedOrWithinDifference() throws IOException {
    Query q = new IntervalQuery(field, Intervals.phrase(
        Intervals.term("coordinate"),
        Intervals.notContaining(
            Intervals.or(Intervals.phrase("genome", "mapping"), Intervals.term("genome")),
            Intervals.term("wibble")),
        Intervals.term("research")));
    checkHits(q, new int[]{ 6, 7 });
  }

  public void testNestedOrWithinConjunctionFilter() throws IOException {
    Query q = new IntervalQuery(field, Intervals.phrase(
        Intervals.term("coordinate"),
        Intervals.containing(
            Intervals.or(Intervals.phrase("genome", "mapping"), Intervals.term("genome")),
            Intervals.term("genome")),
        Intervals.term("research")));
    checkHits(q, new int[]{ 6, 7 });

    q = new IntervalQuery(field, Intervals.phrase(
        Intervals.term("greater"),
        Intervals.or(Intervals.phrase("new", "york"), Intervals.term("york"))));
    checkHits(q, new int[]{ 8 });
  }

  public void testUnordered() throws IOException {
    Query q = new IntervalQuery(field,
        Intervals.unordered(
            Intervals.term("w1"),
            Intervals.ordered(
                Intervals.term("w3"),
                Intervals.term("yy")
            )
        )
    );
    checkHits(q, new int[]{3});
  }

  public void testUnorderedNoOverlaps() throws IOException {
    Query q = new IntervalQuery(field,
        Intervals.maxgaps(3, Intervals.unorderedNoOverlaps(Intervals.term("addressed"),
          Intervals.maxgaps(5, Intervals.unorderedNoOverlaps(Intervals.term("message"),
            Intervals.maxgaps(3, Intervals.unordered(Intervals.term("intend"), Intervals.term("message"))))))));
    checkHits(q, new int[]{ 9 });

    q = new IntervalQuery(field, Intervals.unorderedNoOverlaps(
        Intervals.term("w2"),
        Intervals.or(Intervals.term("w2"), Intervals.term("w3"))));
    checkHits(q, new int[]{ 0, 1, 2, 3, 5 });
  }

  public void testNestedOrInUnorderedMaxGaps() throws IOException {
    Query q = new IntervalQuery(field, Intervals.maxgaps(1, Intervals.unordered(
            Intervals.or(Intervals.term("coordinate"), Intervals.phrase("coordinate", "genome")),
            Intervals.term("research"))
    ));
    checkHits(q, new int[]{ 6, 7 });
  }

  public void testOrderedWithGaps() throws IOException {
    Query q = new IntervalQuery(field, Intervals.maxgaps(1, Intervals.ordered(
            Intervals.term("issue"), Intervals.term("search"), Intervals.term("ordered")
    )));
    checkHits(q, new int[]{});
  }

  public void testNestedOrInContainedBy() throws IOException {
    Query q = new IntervalQuery(field, Intervals.containedBy(
        Intervals.term("genome"),
        Intervals.or(Intervals.term("coordinate"), Intervals.ordered(Intervals.term("coordinate"), Intervals.term("research")))));
    checkHits(q, new int[]{ 6, 7 });
  }

  public void testDefinedGaps() throws IOException {
    Query q = new IntervalQuery(field,
        Intervals.phrase(Intervals.term("w1"), Intervals.extend(Intervals.term("w2"), 1, 0)));
    checkHits(q, new int[]{ 1, 2, 5 });
  }

  public void testScoring() throws IOException {

    IntervalsSource source = Intervals.ordered(Intervals.or(Intervals.term("w1"), Intervals.term("w2")), Intervals.term("w3"));

    Query q = new IntervalQuery(field, source);
    TopDocs td = searcher.search(q, 10);
    assertEquals(5, td.totalHits.value);
    assertEquals(1, td.scoreDocs[0].doc);
    assertEquals(3, td.scoreDocs[1].doc);
    assertEquals(0, td.scoreDocs[2].doc);
    assertEquals(5, td.scoreDocs[3].doc);
    assertEquals(2, td.scoreDocs[4].doc);

    Query boostQ = new BoostQuery(q, 2);
    TopDocs boostTD = searcher.search(boostQ, 10);
    assertEquals(5, boostTD.totalHits.value);
    for (int i = 0; i < 5; i++) {
      assertEquals(td.scoreDocs[i].score * 2, boostTD.scoreDocs[i].score, 0);
    }

    // change the pivot - order should remain the same
    Query q1 = new IntervalQuery(field, source, 2);
    TopDocs td1 = searcher.search(q1, 10);
    assertEquals(5, td1.totalHits.value);
    assertEquals(0.5f, td1.scoreDocs[0].score, 0);  // freq=pivot
    for (int i = 0; i < 5; i++) {
      assertEquals(td.scoreDocs[i].doc, td1.scoreDocs[i].doc);
    }

    // increase the exp, docs higher than pivot should get a higher score, and vice versa
    Query q2 = new IntervalQuery(field, source, 1.2f, 2f);
    TopDocs td2 = searcher.search(q2, 10);
    assertEquals(5, td2.totalHits.value);
    for (int i = 0; i < 5; i++) {
      assertEquals(td.scoreDocs[i].doc, td2.scoreDocs[i].doc);
      if (i < 2) {
        assertTrue(td.scoreDocs[i].score < td2.scoreDocs[i].score);
      }
      else {
        assertTrue(td.scoreDocs[i].score > td2.scoreDocs[i].score);
      }
    }

    // check valid bounds
    expectThrows(IllegalArgumentException.class, () -> new IntervalQuery(field, source, -1));
    expectThrows(IllegalArgumentException.class, () -> new IntervalQuery(field, source, 1, -1f));
  }

}
