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
import org.apache.lucene.search.Query;
import org.apache.lucene.search.posfilter.OrderedNearQuery;
import org.apache.lucene.search.posfilter.RangeFilterQuery;
import org.junit.Test;

import java.io.IOException;

public class TestRangeFilterQuery extends IntervalTestBase {

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
      "w1 w3 w4 w5 w6 w7 w8 w4", //1
      "w1 w3 w10 w4 w5 w6 w7 w8", //2
      "w1 w3 w2 w4 w10 w5 w6 w7 w8", //3
  };

  @Test
  public void testSimpleTermRangeFilter() throws IOException {
    Query q = new RangeFilterQuery(2, makeTermQuery("w4"));
    checkIntervals(q, searcher, new int[][]{
        { 1, 2, 2 }
    });
  }

  @Test
  public void testStartEndTermRangeFilter() throws IOException {
    Query q = new RangeFilterQuery(2, 4, makeTermQuery("w3"));
    checkIntervals(q, searcher, new int[][]{
        { 0, 2, 2 }
    });
  }

  public void testRangeFilteredPositionFilter() throws IOException {
    Query q = new OrderedNearQuery(0, makeTermQuery("w4"), makeTermQuery("w5"));
    q = new RangeFilterQuery(3, 10, q);
    checkIntervals(q, searcher, new int[][]{
        { 0, 3, 4 },
        { 2, 3, 4 }
    });
  }
}
