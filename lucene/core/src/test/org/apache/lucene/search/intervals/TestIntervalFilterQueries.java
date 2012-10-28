package org.apache.lucene.search.intervals;

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

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.search.intervals.OrderedNearQuery;
import org.apache.lucene.search.intervals.UnorderedNearQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

import java.io.IOException;

public class TestIntervalFilterQueries extends LuceneTestCase {

  private IndexSearcher searcher;
  private IndexReader reader;
  private Directory directory;

  public static final String field = "field";

  @Override
  public void setUp() throws Exception {
    super.setUp();
    directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory,
        newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()))
            .setMergePolicy(newLogMergePolicy()));
    for (int i = 0; i < docFields.length; i++) {
      Document doc = new Document();
      doc.add(newField(field, docFields[i], TextField.TYPE_STORED));
      writer.addDocument(doc);
    }
    writer.forceMerge(1);
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
    "w1 w2 w3 w4 w5 w6 w7 w8 w9 w10 w11 w12", //0
    "w1 w3 w4 w5 w6 w7 w8", //1
    "w1 w3 w10 w4 w5 w6 w7 w8", //2
    "w1 w3 w2 w4 w5 w6 w7 w8", //3
  };

  public TermQuery makeTermQuery(String text) {
    return new TermQuery(new Term(field, text));
  }

  private void checkHits(Query query, int[] results) throws IOException {
    CheckHits.checkHits(random(), query, field, searcher, results);
  }

  // or(w1 pre/2 w2, w1 pre/3 w10)
  public void testOrNearNearQuery() throws IOException {
    Query near1 = new OrderedNearQuery(2, makeTermQuery("w1"), makeTermQuery("w2"));
    Query near2 = new OrderedNearQuery(3, makeTermQuery("w1"), makeTermQuery("w10"));
    BooleanQuery bq = new BooleanQuery();
    bq.add(near1, BooleanClause.Occur.SHOULD);
    bq.add(near2, BooleanClause.Occur.SHOULD);

    checkHits(bq, new int[] { 0, 2, 3 });
  }

  // or(w2 within/2 w1, w10 within/3 w1)
  public void testUnorderedNearNearQuery() throws IOException {
    Query near1 = new UnorderedNearQuery(2, makeTermQuery("w2"), makeTermQuery("w1"));
    Query near2 = new UnorderedNearQuery(3, makeTermQuery("w10"), makeTermQuery("w1"));
    BooleanQuery bq = new BooleanQuery();
    bq.add(near1, BooleanClause.Occur.SHOULD);
    bq.add(near2, BooleanClause.Occur.SHOULD);

    checkHits(bq, new int[] { 0, 2, 3 });
  }

  // (a pre/2 b) pre/6 (c pre/2 d)
  public void testNearNearNearQuery() throws IOException {
    Query near1 = new OrderedNearQuery(2, makeTermQuery("w1"), makeTermQuery("w4"));
    Query near2 = new OrderedNearQuery(2, makeTermQuery("w10"), makeTermQuery("w12"));
    Query near3 = new OrderedNearQuery(6, near1, near2);
    checkHits(near3, new int[] { 0 });
  }
}
