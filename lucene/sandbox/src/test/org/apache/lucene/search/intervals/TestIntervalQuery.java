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

package org.apache.lucene.search.intervals;

import java.io.IOException;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.CheckHits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Ignore;

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
      "coordinate genome research"
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

  // The Vigna paper doesn't deal with prefix disjunctions.  For now, we keep the same
  // logic as detailed in the paper, but we may want to address it in future so that tests
  // like the one below will pass
  @Ignore
  public void testNestedOr() throws IOException {
    Query q = new IntervalQuery(field, Intervals.phrase(
        Intervals.term("coordinate"),
        Intervals.or(Intervals.phrase("genome", "mapping"), Intervals.term("genome")),
        Intervals.term("research")));
    checkHits(q, new int[]{ 6, 7 });
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
}
