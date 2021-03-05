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
package org.apache.solr.search;

import org.apache.lucene.index.Term;
import org.apache.solr.legacy.LegacyNumericRangeQuery;
import org.apache.lucene.search.*;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Collections;

public class TestMaxScoreQueryParser extends SolrTestCaseJ4 {
  Query q;
  BooleanClause[] clauses;

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }

  @Test
  public void testFallbackToLucene() {
    q = parse("foo");
    assertEquals(new TermQuery(new Term("text", "foo")), q);

    q = parse("foo^3.0");
    assertEquals(new BoostQuery(new TermQuery(new Term("text", "foo")), 3f), q);

    q = parse("price:[0 TO 10]");
    @SuppressWarnings({"rawtypes"})
    Class expected = LegacyNumericRangeQuery.class;
    if (Boolean.getBoolean(NUMERIC_POINTS_SYSPROP)) {
      expected = PointRangeQuery.class;
      if (Boolean.getBoolean(NUMERIC_DOCVALUES_SYSPROP)) {
        expected = IndexOrDocValuesQuery.class;
      }
    }
    assertTrue(expected + " vs actual: " + q.getClass(), expected.isInstance(q));
  }

  @Test
  public void testNoShouldClauses() {
    q = parse("+foo +bar");
    clauses = clauses(q);
    assertEquals(2, clauses.length);
    assertTrue(clauses[0].isRequired());
    assertTrue(clauses[1].isRequired());

    q = parse("+foo -bar");
    clauses = clauses(q);
    assertEquals(2, clauses.length);
    assertTrue(clauses[0].isRequired());
    assertTrue(clauses[1].isProhibited());
  }

  @Test
  public void testPureMax() {
    q = parse("foo bar");
    clauses = clauses(q);
    assertEquals(1, clauses.length);
    assertTrue(clauses[0].getQuery() instanceof DisjunctionMaxQuery);
    assertEquals(0.0, ((DisjunctionMaxQuery) clauses[0].getQuery()).getTieBreakerMultiplier(), 1e-15);
    List<Query> qa = ((DisjunctionMaxQuery) clauses[0].getQuery()).getDisjuncts();
    assertEquals(2, qa.size());
    assertEquals("text:foo", qa.get(0).toString());
  }

  @Test
  public void testMaxAndProhibited() {
    q = parse("foo bar -baz");
    clauses = clauses(q);
    assertEquals(2, clauses.length);
    assertTrue(clauses[0].getQuery() instanceof DisjunctionMaxQuery);
    assertTrue(clauses[1].getQuery() instanceof TermQuery);
    assertEquals("text:baz", clauses[1].getQuery().toString());
    assertTrue(clauses[1].isProhibited());
  }

  @Test
  public void testTie() {
    q = parse("foo bar", "tie", "0.5");
    clauses = clauses(q);
    assertEquals(1, clauses.length);
    assertTrue(clauses[0].getQuery() instanceof DisjunctionMaxQuery);
    assertEquals(0.5, ((DisjunctionMaxQuery) clauses[0].getQuery()).getTieBreakerMultiplier(), 1e-15);
  }

  @Test
  public void testBoost() {
    // Simple term query
    q = parse("foo^3.0");
    assertTrue(q instanceof BoostQuery);
    assertEquals(3.0, ((BoostQuery) q).getBoost(), 1e-15);

    // Some DMQ and one plain required
    q = parse("foo^5.0 bar^6.0 +baz^7");
    clauses = clauses(q);
    assertEquals(2, clauses.length);
    assertTrue(clauses[0].getQuery() instanceof DisjunctionMaxQuery);
    DisjunctionMaxQuery dmq = ((DisjunctionMaxQuery) clauses[0].getQuery());
    Query fooClause = ((BooleanQuery)dmq.getDisjuncts().get(0)).clauses().iterator().next().getQuery();
    assertEquals(5.0, ((BoostQuery) fooClause).getBoost(), 1e-15);
    Query barClause = ((BooleanQuery)dmq.getDisjuncts().get(1)).clauses().iterator().next().getQuery();
    assertEquals(6.0, ((BoostQuery) barClause).getBoost(), 1e-15);
    assertEquals(7.0, ((BoostQuery) clauses[1].getQuery()).getBoost(), 1e-15);
    assertFalse(q instanceof BoostQuery);

    // Grouped with parens on top level
    q = parse("(foo^2.0 bar)^3.0");
    clauses = clauses(q);
    assertEquals(1, clauses.length);
    assertTrue(clauses[0].getQuery() instanceof DisjunctionMaxQuery);
    dmq = ((DisjunctionMaxQuery) clauses[0].getQuery());
    fooClause = ((BooleanQuery)dmq.getDisjuncts().get(0)).clauses().iterator().next().getQuery();
    assertEquals(2.0, ((BoostQuery) fooClause).getBoost(), 1e-15);
    barClause = ((BooleanQuery)dmq.getDisjuncts().get(1)).clauses().iterator().next().getQuery();
    assertFalse(barClause instanceof BoostQuery);
    assertEquals(3.0, ((BoostQuery) q).getBoost(), 1e-15);
  }

  //
  // Helper methods
  //

  private Query parse(String q, String... params) {
    try {
      ModifiableSolrParams p = new ModifiableSolrParams();
      ArrayList<String> al = new ArrayList<>(Arrays.asList(params));
      while(al.size() >= 2) {
        p.add(al.remove(0), al.remove(0));
      }
      return new MaxScoreQParser(q, p, new MapSolrParams(Collections.singletonMap("df", "text")), req(q)).parse();
    } catch (SyntaxError syntaxError) {
      fail("Failed with exception "+syntaxError.getMessage());
    }
    fail("Parse failed");
    return null;
  }

  private BooleanClause[] clauses(Query q) {
    while (q instanceof BoostQuery) {
      q = ((BoostQuery) q).getQuery();
    }
    return ((BooleanQuery) q).clauses().toArray(new BooleanClause[0]);
  }
}
