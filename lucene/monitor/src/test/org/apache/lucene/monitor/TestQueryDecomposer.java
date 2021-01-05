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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

public class TestQueryDecomposer extends MonitorTestBase {

  private static final QueryDecomposer decomposer = new QueryDecomposer();

  public void testConjunctionsAreNotDecomposed() {
    Query q = parse("+hello world");
    Set<Query> expected = Collections.singleton(parse("+hello world"));
    assertEquals(expected, decomposer.decompose(q));
  }

  public void testSimpleDisjunctions() {
    Query q = parse("hello world");
    Set<Query> expected = new HashSet<>(Arrays.asList(parse("hello"), parse("world")));
    assertEquals(expected, decomposer.decompose(q));
  }

  public void testNestedDisjunctions() {
    Query q = parse("(hello goodbye) world");
    Set<Query> expected =
        new HashSet<>(Arrays.asList(parse("hello"), parse("goodbye"), parse("world")));
    assertEquals(expected, decomposer.decompose(q));
  }

  public void testExclusions() {
    Set<Query> expected =
        new HashSet<>(Arrays.asList(parse("+hello -goodbye"), parse("+world -goodbye")));
    assertEquals(expected, decomposer.decompose(parse("hello world -goodbye")));
  }

  public void testNestedExclusions() {
    Set<Query> expected =
        new HashSet<>(
            Arrays.asList(
                parse("+(+hello -goodbye) -greeting"), parse("+(+world -goodbye) -greeting")));
    assertEquals(expected, decomposer.decompose(parse("((hello world) -goodbye) -greeting")));
  }

  public void testSingleValuedConjunctions() {
    Set<Query> expected = new HashSet<>(Arrays.asList(parse("hello"), parse("world")));
    assertEquals(expected, decomposer.decompose(parse("+(hello world)")));
  }

  public void testSingleValuedConjunctWithExclusions() {
    Set<Query> expected =
        new HashSet<>(Arrays.asList(parse("+hello -goodbye"), parse("+world -goodbye")));
    assertEquals(expected, decomposer.decompose(parse("+(hello world) -goodbye")));
  }

  public void testBoostsArePreserved() {
    Set<Query> expected = new HashSet<>(Arrays.asList(parse("hello^0.7"), parse("world^0.7")));
    assertEquals(expected, decomposer.decompose(parse("+(hello world)^0.7")));
    expected =
        new HashSet<>(Arrays.asList(parse("+hello^0.7 -goodbye"), parse("+world^0.7 -goodbye")));
    assertEquals(expected, decomposer.decompose(parse("+(hello world)^0.7 -goodbye")));
    expected = new HashSet<>(Arrays.asList(parse("(hello^0.5)^0.8"), parse("world^0.8")));
    assertEquals(expected, decomposer.decompose(parse("+(hello^0.5 world)^0.8")));
  }

  public void testDisjunctionMaxDecomposition() {
    Query q =
        new DisjunctionMaxQuery(
            Arrays.asList(new TermQuery(new Term("f", "t1")), new TermQuery(new Term("f", "t2"))),
            0.1f);
    Set<Query> expected = new HashSet<>(Arrays.asList(parse("f:t1"), parse("f:t2")));
    assertEquals(expected, decomposer.decompose(q));
  }

  public void testNestedDisjunctionMaxDecomposition() {
    Query q = new DisjunctionMaxQuery(Arrays.asList(parse("hello goodbye"), parse("world")), 0.1f);
    Set<Query> expected =
        new HashSet<>(Arrays.asList(parse("hello"), parse("goodbye"), parse("world")));
    assertEquals(expected, decomposer.decompose(q));
  }

  public void testFilterAndShouldClause() {
    final Query shouldTermQuery = new TermQuery(new Term("f", "should"));
    final Query filterTermQuery = new TermQuery(new Term("f", "filter"));
    Query q =
        new BooleanQuery.Builder()
            .add(shouldTermQuery, BooleanClause.Occur.SHOULD)
            .add(filterTermQuery, BooleanClause.Occur.FILTER)
            .build();

    assertEquals(Collections.singleton(q), decomposer.decompose(q));
  }
}
