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

package org.apache.lucene.concordance;

import java.io.IOException;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.spans.SimpleSpanQueryConverter;
import org.apache.lucene.search.spans.SpanOrQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestSpanQueryConverter {

  @Test
  public void testMultiTerm() throws IOException {
    //test to make sure multiterm returns empty query for different field
    String f1 = "f1";
    String f2 = "f2";
    Query q = new PrefixQuery(new Term(f1, "f*"));
    SimpleSpanQueryConverter c = new SimpleSpanQueryConverter();
    SpanQuery sq = c.convert(f2, q);
    assertTrue(sq instanceof SpanOrQuery);
    assertEquals(0, ((SpanOrQuery)sq).getClauses().length);
  }
  //TODO: add more tests
}
