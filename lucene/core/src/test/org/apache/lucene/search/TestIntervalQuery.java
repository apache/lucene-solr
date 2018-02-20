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

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
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
      "w1 xx w2 yy w3",
      "w1 w3 xx w2 yy w3",
      "w2 w1",
      "w2 w1 w3 w2"
  };

  private void checkHits(Query query, int[] results) throws IOException {
    CheckHits.checkHits(random(), query, field, searcher, results);
  }

  public void testOrderedNearQueryWidth0() throws IOException {
    checkHits(IntervalQuery.orderedNearQuery(field, 0, new TermQuery(new Term(field, "w1")),
        new TermQuery(new Term(field, "w2"))),
        new int[]{0});
  }

  public void testOrderedNearQueryWidth1() throws IOException {
    checkHits(IntervalQuery.orderedNearQuery(field, 1, new TermQuery(new Term(field, "w1")),
        new TermQuery(new Term(field, "w2"))),
        new int[]{0, 1, 2, 5});
  }

  public void testOrderedNearQueryWidth2() throws IOException {
    checkHits(IntervalQuery.orderedNearQuery(field, 2, new TermQuery(new Term(field, "w1")),
        new TermQuery(new Term(field, "w2"))),
        new int[]{0, 1, 2, 3, 5});
  }

  public void testNestedOrderedNearQuery() throws IOException {
    // onear/1(w1, onear/2(w2, w3))
    Query q = IntervalQuery.orderedNearQuery(field, 1,
        new TermQuery(new Term(field, "w1")),
        IntervalQuery.orderedNearQuery(field, 2,
            new TermQuery(new Term(field, "w2")),
            new TermQuery(new Term(field, "w3")))
    );

    checkHits(q, new int[]{0, 1, 2});
  }

}
