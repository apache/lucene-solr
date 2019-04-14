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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.index.Term;
import org.apache.lucene.luwak.queryparsers.LuceneQueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.LuceneTestCase;

public class TestQueryDecomposer extends LuceneTestCase  {

  private static final QueryDecomposer decomposer = new QueryDecomposer();
  private static final LuceneQueryParser PARSER = new LuceneQueryParser("field");

  public static Query q(String q) throws Exception {
    return PARSER.parse(q, Collections.emptyMap());
  }

  public void testConjunctionsAreNotDecomposed() throws Exception {
    Query q = q("+hello world");
    Set<Query> expected = Collections.singleton(q("+hello world"));
    assertEquals(expected, decomposer.decompose(q));
  }

  public void testSimpleDisjunctions() throws Exception {
    Query q = q("hello world");
    Set<Query> expected = new HashSet<>(Arrays.asList(q("hello"), q("world")));
    assertEquals(expected, decomposer.decompose(q));
  }

  public void testNestedDisjunctions() throws Exception {
    Query q = q("(hello goodbye) world");
    Set<Query> expected = new HashSet<>(Arrays.asList(q("hello"), q("goodbye"), q("world")));
    assertEquals(expected, decomposer.decompose(q));
  }

  public void testExclusions() throws Exception {
    Set<Query> expected = new HashSet<>(Arrays.asList(q("+hello -goodbye"), q("+world -goodbye")));
    assertEquals(expected, decomposer.decompose(q("hello world -goodbye")));
  }

  public void testNestedExclusions() throws Exception {
    Set<Query> expected
        = new HashSet<>(Arrays.asList(q("+(+hello -goodbye) -greeting"), q("+(+world -goodbye) -greeting")));
    assertEquals(expected, decomposer.decompose(q("((hello world) -goodbye) -greeting")));
  }

  public void testSingleValuedConjunctions() throws Exception {
    Set<Query> expected = new HashSet<>(Arrays.asList(q("hello"), q("world")));
    assertEquals(expected, decomposer.decompose(q("+(hello world)")));
  }

  public void testSingleValuedConjunctWithExclusions() throws Exception {
    Set<Query> expected = new HashSet<>(Arrays.asList(q("+hello -goodbye"), q("+world -goodbye")));
    assertEquals(expected, decomposer.decompose(q("+(hello world) -goodbye")));
  }

  public void testBoostsArePreserved() throws Exception {
    Set<Query> expected = new HashSet<>(Arrays.asList(q("hello^0.7"), q("world^0.7")));
    assertEquals(expected, decomposer.decompose(q("+(hello world)^0.7")));
    expected = new HashSet<>(Arrays.asList(q("+hello^0.7 -goodbye"), q("+world^0.7 -goodbye")));
    assertEquals(expected, decomposer.decompose(q("+(hello world)^0.7 -goodbye")));
    expected = new HashSet<>(Arrays.asList(q("(hello^0.5)^0.8"), q("world^0.8")));
    assertEquals(expected, decomposer.decompose(q("+(hello^0.5 world)^0.8")));
  }

  public void testDisjunctionMaxDecomposition() throws Exception {
    Query q = new DisjunctionMaxQuery(
        Arrays.asList(new TermQuery(new Term("f", "t1")), new TermQuery(new Term("f", "t2"))), 0.1f
    );
    Set<Query> expected = new HashSet<>(Arrays.asList(q("f:t1"), q("f:t2")));
    assertEquals(expected, decomposer.decompose(q));
  }

  public void testNestedDisjunctionMaxDecomposition() throws Exception {
    Query q = new DisjunctionMaxQuery(
        Arrays.asList(q("hello goodbye"), q("world")), 0.1f
    );
    Set<Query> expected = new HashSet<>(Arrays.asList(q("hello"), q("goodbye"), q("world")));
    assertEquals(expected, decomposer.decompose(q));
  }

  public void testFilterAndShouldClause() {
    final Query shouldTermQuery = new TermQuery(new Term("f", "should"));
    final Query filterTermQuery = new TermQuery(new Term("f", "filter"));
    Query q = new BooleanQuery.Builder()
        .add(shouldTermQuery, BooleanClause.Occur.SHOULD)
        .add(filterTermQuery, BooleanClause.Occur.FILTER)
        .build();

    assertEquals(Collections.singleton(q), decomposer.decompose(q));
  }
}
