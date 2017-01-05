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
package org.apache.lucene.search;


import java.io.IOException;

import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.util.LuceneTestCase;

public class TestBoostQuery extends LuceneTestCase {

  public void testEquals() {
    final float boost = random().nextFloat() * 3 - 1;
    BoostQuery q1 = new BoostQuery(new MatchAllDocsQuery(), boost);
    BoostQuery q2 = new BoostQuery(new MatchAllDocsQuery(), boost);
    assertEquals(q1, q2);
    assertEquals(q1.getBoost(), q2.getBoost(), 0f);

    float boost2 = boost;
    while (boost == boost2) {
      boost2 = random().nextFloat() * 3 - 1;
    }
    BoostQuery q3 = new BoostQuery(new MatchAllDocsQuery(), boost2);
    assertFalse(q1.equals(q3));
    assertFalse(q1.hashCode() == q3.hashCode());
  }

  public void testToString() {
    assertEquals("(foo:bar)^2.0", new BoostQuery(new TermQuery(new Term("foo", "bar")), 2).toString());
    BooleanQuery bq = new BooleanQuery.Builder()
        .add(new TermQuery(new Term("foo", "bar")), Occur.SHOULD)
        .add(new TermQuery(new Term("foo", "baz")), Occur.SHOULD)
        .build();
    assertEquals("(foo:bar foo:baz)^2.0", new BoostQuery(bq, 2).toString());
  }

  public void testRewrite() throws IOException {
    IndexSearcher searcher = new IndexSearcher(new MultiReader());

    // inner queries are rewritten
    Query q = new BoostQuery(new BooleanQuery.Builder().build(), 2);
    assertEquals(new BoostQuery(new MatchNoDocsQuery(), 2), searcher.rewrite(q));

    // boosts are merged
    q = new BoostQuery(new BoostQuery(new MatchAllDocsQuery(), 3), 2);
    assertEquals(new BoostQuery(new MatchAllDocsQuery(), 6), searcher.rewrite(q));

    // scores are not computed when the boost is 0
    q = new BoostQuery(new MatchAllDocsQuery(), 0);
    assertEquals(new BoostQuery(new ConstantScoreQuery(new MatchAllDocsQuery()), 0), searcher.rewrite(q));
  }

}
