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

package org.apache.lucene.luwak;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;

public class TestQueryTermFilter extends LuceneTestCase {

  private static final String FIELD = "f";

  private static List<Indexable> indexable(String id, String query) {
    QueryCacheEntry e = new QueryCacheEntry(
        new BytesRef(id.getBytes(StandardCharsets.UTF_8)),
        new TermQuery(new Term(FIELD, query)),
        Collections.emptyMap()
    );
    Document doc = new Document();
    doc.add(new StringField(FIELD, query, Field.Store.NO));
    return Collections.singletonList(
        new Indexable(id, e, doc)
    );
  }

  public void testFiltersAreRemoved() throws IOException {

    QueryIndex qi = new QueryIndex();
    qi.commit(indexable("1", "term"));
    assertEquals(1, qi.termFilters.size());
    qi.commit(indexable("2", "term2"));
    assertEquals(1, qi.termFilters.size());

    QueryTermFilter tf = qi.termFilters.values().iterator().next();
    assertNotNull(tf);
    assertEquals(2, tf.getTerms(FIELD).size());
  }

}
