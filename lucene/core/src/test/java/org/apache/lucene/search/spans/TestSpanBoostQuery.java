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
import org.apache.lucene.util.LuceneTestCase;


public class TestSpanBoostQuery extends LuceneTestCase {

  public void testEquals() {
    final float boost = random().nextFloat() * 3 - 1;
    SpanTermQuery q = new SpanTermQuery(new Term("foo", "bar"));
    SpanBoostQuery q1 = new SpanBoostQuery(q, boost);
    SpanBoostQuery q2 = new SpanBoostQuery(q, boost);
    assertEquals(q1, q2);
    assertEquals(q1.getBoost(), q2.getBoost(), 0f);
 
    float boost2 = boost;
    while (boost == boost2) {
      boost2 = random().nextFloat() * 3 - 1;
    }
    SpanBoostQuery q3 = new SpanBoostQuery(q, boost2);
    assertFalse(q1.equals(q3));
    assertFalse(q1.hashCode() == q3.hashCode());
  }

  public void testToString() {
    assertEquals("(foo:bar)^2.0", new SpanBoostQuery(new SpanTermQuery(new Term("foo", "bar")), 2).toString());
    SpanOrQuery bq = new SpanOrQuery(
        new SpanTermQuery(new Term("foo", "bar")),
        new SpanTermQuery(new Term("foo", "baz")));
    assertEquals("(spanOr([foo:bar, foo:baz]))^2.0", new SpanBoostQuery(bq, 2).toString());
  }

}
