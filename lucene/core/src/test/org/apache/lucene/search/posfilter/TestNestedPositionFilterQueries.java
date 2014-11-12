package org.apache.lucene.search.posfilter;

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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;

import java.io.IOException;

public class TestNestedPositionFilterQueries extends IntervalTestBase {

  @Override
  protected void addDocs(RandomIndexWriter writer) throws IOException {
    for (int i = 0; i < docFields.length; i++) {
      Document doc = new Document();
      doc.add(newField("field", docFields[i], TextField.TYPE_STORED));
      writer.addDocument(doc);
    }
  }

  private String[] docFields = {
    "w1 w2 w3 w4 w5 w6 w7 w8 w9 w10 w11 w12", //0
    "w1 w3 w4 w5 w6 w7 w8", //1
    "w1 w3 w10 w4 w5 w6 w7 w8", //2
    "w1 w3 w2 w4 w5 w6 w7 w8", //3
  };

  public void testOrderedDisjunctionQueries() throws IOException {
    // Two phrases whose subparts appear in a document, but that do not fulfil the slop
    // requirements of the parent IntervalFilterQuery
    Query sentence1 = new OrderedNearQuery(0, makeTermQuery("w1"), makeTermQuery("w8"), makeTermQuery("w4"));
    Query sentence2 = new OrderedNearQuery(0, makeTermQuery("w3"), makeTermQuery("w7"), makeTermQuery("w6"));
    BooleanQuery bq = new BooleanQuery();
    bq.add(sentence1, BooleanClause.Occur.SHOULD);
    bq.add(sentence2, BooleanClause.Occur.SHOULD);
    checkIntervals(bq, searcher, new int[][]{});
  }

  public void testFilterDisjunctionQuery() throws IOException {
    Query near1 = makeTermQuery("w4");
    Query near2 = new OrderedNearQuery(3, makeTermQuery("w1"), makeTermQuery("w10"));
    BooleanQuery bq = new BooleanQuery();
    bq.add(near1, BooleanClause.Occur.SHOULD);
    bq.add(near2, BooleanClause.Occur.SHOULD);
    checkIntervals(bq, searcher, new int[][]{
        { 0, 3, 3 },
        { 1, 2, 2 },
        { 2, 0, 2, 3, 3 },
        { 3, 3, 3 }
    });
  }

  // or(w1 pre/2 w2, w1 pre/3 w10)
  public void testOrNearNearQuery() throws IOException {
    Query near1 = new OrderedNearQuery(2, makeTermQuery("w1"), makeTermQuery("w2"));
    Query near2 = new OrderedNearQuery(3, makeTermQuery("w1"), makeTermQuery("w10"));
    BooleanQuery bq = new BooleanQuery();
    bq.add(near1, BooleanClause.Occur.SHOULD);
    bq.add(near2, BooleanClause.Occur.SHOULD);
    checkIntervals(bq, searcher, new int[][]{
        { 0, 0, 1 },
        { 2, 0, 2 },
        { 3, 0, 2 }
    });
  }

  // or(w2 within/2 w1, w10 within/3 w1)
  public void testUnorderedNearNearQuery() throws IOException {
    Query near1 = new UnorderedNearQuery(2, makeTermQuery("w2"), makeTermQuery("w1"));
    Query near2 = new UnorderedNearQuery(3, makeTermQuery("w10"), makeTermQuery("w1"));
    BooleanQuery bq = new BooleanQuery();
    bq.add(near1, BooleanClause.Occur.SHOULD);
    bq.add(near2, BooleanClause.Occur.SHOULD);
    checkIntervals(bq, searcher, new int[][]{
        {0, 0, 1},
        {2, 0, 2},
        {3, 0, 2}
    });
  }

  // (a pre/2 b) pre/6 (c pre/2 d)
  public void testNearNearNearQuery() throws IOException {
    Query near1 = new OrderedNearQuery(2, makeTermQuery("w1"), makeTermQuery("w4"));
    Query near2 = new OrderedNearQuery(2, makeTermQuery("w10"), makeTermQuery("w12"));
    Query near3 = new OrderedNearQuery(6, near1, near2);
    checkIntervals(near3, searcher, new int[][]{
        { 0, 0, 11 }
    });
  }

  public void testOrNearNearNonExistentQuery() throws IOException {
    Query near1 = new OrderedNearQuery(2, makeTermQuery("w1"), makeTermQuery("w12"));
    Query near2 = new OrderedNearQuery(2, makeTermQuery("w3"), makeTermQuery("w8"));
    BooleanQuery bq = new BooleanQuery();
    bq.add(near1, BooleanClause.Occur.SHOULD);
    bq.add(near2, BooleanClause.Occur.SHOULD);
    BooleanQuery wrapper = new BooleanQuery();
    wrapper.add(bq, BooleanClause.Occur.MUST);
    wrapper.add(makeTermQuery("foo"), BooleanClause.Occur.MUST_NOT);
    checkIntervals(wrapper, searcher, new int[][]{});
  }

}
