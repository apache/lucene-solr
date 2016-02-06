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


import org.apache.lucene.index.Term;
import org.apache.lucene.search.QueryUtils;
import org.apache.lucene.util.LuceneTestCase;

/** Basic tests for SpanOrQuery */
public class TestSpanOrQuery extends LuceneTestCase {
  
  public void testHashcodeEquals() {
    SpanTermQuery q1 = new SpanTermQuery(new Term("field", "foo"));
    SpanTermQuery q2 = new SpanTermQuery(new Term("field", "bar"));
    SpanTermQuery q3 = new SpanTermQuery(new Term("field", "baz"));
    
    SpanOrQuery or1 = new SpanOrQuery(q1, q2);
    SpanOrQuery or2 = new SpanOrQuery(q2, q3);
    QueryUtils.check(or1);
    QueryUtils.check(or2);
    QueryUtils.checkUnequal(or1, or2);
  }
  
  public void testSpanOrEmpty() throws Exception {
    SpanOrQuery a = new SpanOrQuery();
    SpanOrQuery b = new SpanOrQuery();
    assertTrue("empty should equal", a.equals(b));
  }
  
  public void testDifferentField() throws Exception {
    SpanTermQuery q1 = new SpanTermQuery(new Term("field1", "foo"));
    SpanTermQuery q2 = new SpanTermQuery(new Term("field2", "bar"));
    try {
      new SpanOrQuery(q1, q2);
      fail("didn't get expected exception");
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("must have same field"));
    }
  }
}
