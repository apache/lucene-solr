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

package org.apache.lucene.monitor;

import java.io.IOException;
import java.util.Collections;
import java.util.function.BiPredicate;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;

public class TestQueryTermFilter extends LuceneTestCase {

  private static final String FIELD = "f";

  public void testFiltersAreRemoved() throws IOException {

    try (QueryIndex qi = new QueryIndex(new MonitorConfiguration(), new TermFilteredPresearcher())) {
      qi.commit(Collections.singletonList(new MonitorQuery("1", new TermQuery(new Term(FIELD, "term")))));
      assertEquals(1, qi.termFilters.size());
      BiPredicate<String, BytesRef> filter = qi.termFilters.values().iterator().next();
      assertTrue(filter.test(FIELD, new BytesRef("term")));
      assertFalse(filter.test(FIELD, new BytesRef("term2")));

      qi.commit(Collections.singletonList(new MonitorQuery("2", new TermQuery(new Term(FIELD, "term2")))));
      assertEquals(1, qi.termFilters.size());

      filter = qi.termFilters.values().iterator().next();
      assertTrue(filter.test(FIELD, new BytesRef("term")));
      assertTrue(filter.test(FIELD, new BytesRef("term2")));
      assertFalse(filter.test(FIELD, new BytesRef("term3")));
    }
  }

}
