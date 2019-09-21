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
package org.apache.solr.core;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.search.QueryResultKey;
import org.junit.Test;

public class QueryResultKeyTest extends SolrTestCaseJ4 {

  public void testFiltersOutOfOrder1() {
    // the hashcode should be the same even when the list
    // of filters is in a different order
    
    Sort sort = new Sort(new SortField("test", SortField.Type.INT));
    BooleanQuery.Builder query = new BooleanQuery.Builder();
    query.add(new TermQuery(new Term("test", "field")), Occur.MUST);
    
    List<Query> filters = Arrays.<Query>asList(new TermQuery(new Term("test", "field")),
                                               new TermQuery(new Term("test2", "field2")));
    QueryResultKey qrk1 = new QueryResultKey(query.build() , filters, sort, 1);
    
    List<Query> filters2 = Arrays.<Query>asList(new TermQuery(new Term("test2", "field2")),
                                                new TermQuery(new Term("test", "field")));
    QueryResultKey qrk2 = new QueryResultKey(query.build() , filters2, sort, 1);
    assertKeyEquals(qrk1, qrk2);
  }

  @Test
  public void testFiltersOutOfOrder2() {
    Query fq1 = new TermQuery(new Term("test1", "field1"));
    Query fq2 = new TermQuery(new Term("test2", "field2"));

    Query query = new TermQuery(new Term("test3", "field3"));
    List<Query> filters = Arrays.asList(fq1, fq2);

    QueryResultKey key = new QueryResultKey(query, filters, null, 0);

    List<Query> newFilters = Arrays.asList(fq2, fq1);
    QueryResultKey newKey = new QueryResultKey(query, newFilters, null, 0);

    assertKeyEquals(key, newKey);
  }

  public void testQueryResultKeyUnSortedFiltersWithDups() {
    Query query = new TermQuery(new Term("main", "val"));

    // we need Query clauses that have identical hashCodes 
    // but are not equal unless the term is equals
    Query fq_aa = new FlatHashTermQuery("fq_a");
    Query fq_ab = new FlatHashTermQuery("fq_a");
    Query fq_ac = new FlatHashTermQuery("fq_a");
    Query fq_zz = new FlatHashTermQuery("fq_z");

    assertEquals(fq_aa.hashCode(), fq_ab.hashCode());
    assertEquals(fq_aa.hashCode(), fq_ac.hashCode());
    assertEquals(fq_aa.hashCode(), fq_zz.hashCode());

    assertEquals(fq_aa, fq_ab);
    assertEquals(fq_aa, fq_ac);
    assertEquals(fq_ab, fq_aa);
    assertEquals(fq_ab, fq_ac);
    assertEquals(fq_ac, fq_aa);
    assertEquals(fq_ac, fq_ab);

    assertTrue( ! fq_aa.equals(fq_zz) );
    assertTrue( ! fq_ab.equals(fq_zz) );
    assertTrue( ! fq_ac.equals(fq_zz) );
    assertTrue( ! fq_zz.equals(fq_aa) );
    assertTrue( ! fq_zz.equals(fq_ab) );
    assertTrue( ! fq_zz.equals(fq_ac) );

    List<Query> filters1 = Arrays.asList(fq_aa, fq_ab);
    List<Query> filters2 = Arrays.asList(fq_zz, fq_ac);

    QueryResultKey key1 = new QueryResultKey(query, filters1, null, 0);
    QueryResultKey key2 = new QueryResultKey(query, filters2, null, 0);
    
    assertEquals(key1.hashCode(), key2.hashCode());

    assertKeyNotEquals(key1, key2);
  }

  public void testRandomQueryKeyEquality() {


    final int minIters = atLeast(100 * 1000);
    final Query base = new FlatHashTermQuery("base");
    
    // ensure we cover both code paths at least once
    boolean didEquals = false;
    boolean didNotEquals = false;
    int iter = 1;
    while (iter <= minIters || (! didEquals ) || (! didNotEquals ) ) {
      iter++;
      int[] numsA = smallArrayOfRandomNumbers();
      int[] numsB = smallArrayOfRandomNumbers();
      QueryResultKey aa = new QueryResultKey(base, buildFiltersFromNumbers(numsA), null, 0);
      QueryResultKey bb = new QueryResultKey(base, buildFiltersFromNumbers(numsB), null, 0);
      // now that we have our keys, sort the numbers so we know what to expect
      Arrays.sort(numsA);
      Arrays.sort(numsB);
      if (Arrays.equals(numsA, numsB)) {
        didEquals = true;
        assertKeyEquals(aa, bb);
      } else {
        didNotEquals = true;
        assertKeyNotEquals(aa, bb);
      }
    }
    assert minIters <= iter;
  }

  /**
   * does bi-directional equality check as well as verifying hashCode
   */
  public void assertKeyEquals(QueryResultKey key1, QueryResultKey key2) {
    assertNotNull(key1);
    assertNotNull(key2);
    assertEquals(key1.hashCode(), key2.hashCode());
    assertEquals(key1.ramBytesUsed(), key2.ramBytesUsed());
    assertEquals(key1, key2);
    assertEquals(key2, key1);
  }

  /**
   * does bi-directional check that the keys are <em>not</em> equals
   */
  public void assertKeyNotEquals(QueryResultKey key1, QueryResultKey key2) {
    assertTrue( ! key1.equals(key2) );
    assertTrue( ! key2.equals(key1) );
  }

  /**
   * returns a "small" list of "small" random numbers.  The idea behind this method is 
   * that multiple calls have a decent change of returning two arrays which are the 
   * same size and contain the same numbers but in a differed order.
   *
   * the array is guaranteed to always have at least 1 element
   */
  private int[] smallArrayOfRandomNumbers() {
    int size = TestUtil.nextInt(random(), 1, 5);
    int[] result = new int[size];
    for (int i=0; i < size; i++) {
      result[i] = TestUtil.nextInt(random(), 1, 5);
    }
    return result;
  }

  /**
   * Creates an array of Filter queries using {@link FlatHashTermQuery} based on the 
   * specified ints
   */
  private List<Query> buildFiltersFromNumbers(int[] values) {
    ArrayList<Query> filters = new ArrayList<>(values.length);
    for (int val : values) {
      filters.add(new FlatHashTermQuery(String.valueOf(val)));
    }
    return filters;
  }

  /**
   * Quick and dirty subclass of TermQuery that uses fixed field name and a constant 
   * value hashCode, regardless of the Term value.
   */
  private static class FlatHashTermQuery extends TermQuery {
    public FlatHashTermQuery(String val) {
      super(new Term("some_field", val));
    }

    @Override
    public int hashCode() {
      return 42;
    }
  }
}
