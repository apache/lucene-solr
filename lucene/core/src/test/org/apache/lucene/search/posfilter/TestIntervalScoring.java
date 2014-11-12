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
import org.apache.lucene.index.Term;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.posfilter.OrderedNearQuery;
import org.junit.Assert;

import java.io.IOException;

public class TestIntervalScoring extends IntervalTestBase {

  @Override
  protected void addDocs(RandomIndexWriter writer) throws IOException {
    for (String content : docFields) {
      Document doc = new Document();
      doc.add(newField("field", content, TextField.TYPE_NOT_STORED));
      writer.addDocument(doc);
    }
  }

  private String[] docFields = {
      "Should we, could we, would we?",
      "It should -  would it?",
      "It shouldn't",
      "Should we, should we, should we"
  };

  public void testOrderedNearQueryScoring() throws IOException {
    OrderedNearQuery q = new OrderedNearQuery(10, makeTermQuery("should"),
                                                  makeTermQuery("would"));
    checkScores(q, searcher, 1, 0);
  }

  public void testEmptyMultiTermQueryScoring() throws IOException {
    OrderedNearQuery q = new OrderedNearQuery(10, new RegexpQuery(new Term("field", "bar.*")),
                                                  new RegexpQuery(new Term("field", "foo.*")));
    TopDocs docs = searcher.search(q, 10);
    assertEquals(docs.totalHits, 0);
  }


}
