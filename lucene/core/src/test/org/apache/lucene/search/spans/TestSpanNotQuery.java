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
package org.apache.lucene.search.spans;


import java.io.IOException;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.QueryUtils;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

/** Basic tests for SpanNotQuery */
public class TestSpanNotQuery extends LuceneTestCase {
  
  public void testHashcodeEquals() {
    SpanTermQuery q1 = new SpanTermQuery(new Term("field", "foo"));
    SpanTermQuery q2 = new SpanTermQuery(new Term("field", "bar"));
    SpanTermQuery q3 = new SpanTermQuery(new Term("field", "baz"));
    
    SpanNotQuery not1 = new SpanNotQuery(q1, q2);
    SpanNotQuery not2 = new SpanNotQuery(q2, q3);
    QueryUtils.check(not1);
    QueryUtils.check(not2);
    QueryUtils.checkUnequal(not1, not2);
  }
  
  public void testDifferentField() throws Exception {
    SpanTermQuery q1 = new SpanTermQuery(new Term("field1", "foo"));
    SpanTermQuery q2 = new SpanTermQuery(new Term("field2", "bar"));
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      new SpanNotQuery(q1, q2);
    });
    assertTrue(expected.getMessage().contains("must have same field"));
  }
  
  public void testNoPositions() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(new StringField("foo", "bar", Field.Store.NO));
    iw.addDocument(doc);
    
    IndexReader ir = iw.getReader();
    iw.close();
    
    IndexSearcher is = new IndexSearcher(ir);
    SpanTermQuery query = new SpanTermQuery(new Term("foo", "bar"));
    SpanTermQuery query2 = new SpanTermQuery(new Term("foo", "baz"));

    IllegalStateException expected = expectThrows(IllegalStateException.class, () -> {
      is.search(new SpanNotQuery(query, query2), 5);
    });
    assertTrue(expected.getMessage().contains("was indexed without position data"));

    ir.close();
    dir.close();
  }
}
