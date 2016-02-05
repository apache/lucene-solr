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

/** Basic tests for SpanTermQuery */
public class TestSpanTermQuery extends LuceneTestCase {
  
  public void testHashcodeEquals() {
    SpanTermQuery q1 = new SpanTermQuery(new Term("field", "foo"));
    SpanTermQuery q2 = new SpanTermQuery(new Term("field", "bar"));
    QueryUtils.check(q1);
    QueryUtils.check(q2);
    QueryUtils.checkUnequal(q1, q2);
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
    try {
      is.search(query, 5);
      fail("didn't get expected exception");
    } catch (IllegalStateException expected) {
      assertTrue(expected.getMessage().contains("was indexed without position data"));
    }
    ir.close();
    dir.close();
  }
}
