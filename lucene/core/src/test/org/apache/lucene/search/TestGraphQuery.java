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

import org.apache.lucene.index.Term;
import org.apache.lucene.util.LuceneTestCase;

public class TestGraphQuery extends LuceneTestCase {

  public void testEquals() {
    QueryUtils.checkEqual(new GraphQuery(), new GraphQuery());
    QueryUtils.checkEqual(new GraphQuery(new MatchAllDocsQuery()), new GraphQuery(new MatchAllDocsQuery()));
    QueryUtils.checkEqual(
        new GraphQuery(new TermQuery(new Term("a", "a")), new TermQuery(new Term("a", "b"))),
        new GraphQuery(new TermQuery(new Term("a", "a")), new TermQuery(new Term("a", "b")))
    );
  }

  public void testBooleanDetection() {
    assertFalse(new GraphQuery().hasBoolean());
    assertFalse(new GraphQuery(new MatchAllDocsQuery(), new TermQuery(new Term("a", "a"))).hasBoolean());
    assertTrue(new GraphQuery(new BooleanQuery.Builder().build()).hasBoolean());
    assertTrue(new GraphQuery(new TermQuery(new Term("a", "a")), new BooleanQuery.Builder().build()).hasBoolean());
  }

  public void testPhraseDetection() {
    assertFalse(new GraphQuery().hasPhrase());
    assertFalse(new GraphQuery(new MatchAllDocsQuery(), new TermQuery(new Term("a", "a"))).hasPhrase());
    assertTrue(new GraphQuery(new PhraseQuery.Builder().build()).hasPhrase());
    assertTrue(new GraphQuery(new TermQuery(new Term("a", "a")), new PhraseQuery.Builder().build()).hasPhrase());
  }

  public void testToString() {
    assertEquals("Graph(hasBoolean=false, hasPhrase=false)", new GraphQuery().toString());
    assertEquals("Graph(a:a, a:b, hasBoolean=true, hasPhrase=false)",
        new GraphQuery(new TermQuery(new Term("a", "a")),
            new BooleanQuery.Builder().add(new TermQuery(new Term("a", "b")), BooleanClause.Occur.SHOULD)
                .build()).toString());
    assertEquals("Graph(a:\"a b\", a:b, hasBoolean=true, hasPhrase=true)",
        new GraphQuery(
            new PhraseQuery.Builder()
                .add(new Term("a", "a"))
                .add(new Term("a", "b")).build(),
            new BooleanQuery.Builder().add(new TermQuery(new Term("a", "b")), BooleanClause.Occur.SHOULD)
                .build()).toString());
  }

  public void testRewrite() throws IOException {
    QueryUtils.checkEqual(new BooleanQuery.Builder().build(), new GraphQuery().rewrite(null));
    QueryUtils.checkEqual(new TermQuery(new Term("a", "a")),
        new GraphQuery(new TermQuery(new Term("a", "a"))).rewrite(null));
    QueryUtils.checkEqual(
        new BooleanQuery.Builder()
            .add(new TermQuery(new Term("a", "a")), BooleanClause.Occur.SHOULD)
            .add(new TermQuery(new Term("b", "b")), BooleanClause.Occur.SHOULD).build(),
        new GraphQuery(
            new TermQuery(new Term("a", "a")),
            new TermQuery(new Term("b", "b"))
        ).rewrite(null)
    );
  }
}
