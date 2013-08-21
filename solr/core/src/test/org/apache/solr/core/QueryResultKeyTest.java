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

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.search.QueryResultKey;
import org.junit.Test;

public class QueryResultKeyTest extends SolrTestCaseJ4 {

  @Test
  public void testFiltersHashCode() {
    // the hashcode should be the same even when the list
    // of filters is in a different order
    
    Sort sort = new Sort(new SortField("test", SortField.Type.BYTE));
    List<Query> filters = new ArrayList<Query>();
    filters.add(new TermQuery(new Term("test", "field")));
    filters.add(new TermQuery(new Term("test2", "field2")));
    
    BooleanQuery query = new BooleanQuery();
    query.add(new TermQuery(new Term("test", "field")), Occur.MUST);
    
    QueryResultKey qrk1 = new QueryResultKey(query , filters, sort, 1);
    
    List<Query> filters2 = new ArrayList<Query>();
    filters2.add(new TermQuery(new Term("test2", "field2")));
    filters2.add(new TermQuery(new Term("test", "field")));
    QueryResultKey qrk2 = new QueryResultKey(query , filters2, sort, 1);
    
    assertEquals(qrk1.hashCode(), qrk2.hashCode());
  }

  @Test
  public void testQueryResultKeySortedFilters() {
    Query fq1 = new TermQuery(new Term("test1", "field1"));
    Query fq2 = new TermQuery(new Term("test2", "field2"));

    Query query = new TermQuery(new Term("test3", "field3"));
    List<Query> filters = new ArrayList<Query>();
    filters.add(fq1);
    filters.add(fq2);

    QueryResultKey key = new QueryResultKey(query, filters, null, 0);

    List<Query> newFilters = new ArrayList<Query>();
    newFilters.add(fq2);
    newFilters.add(fq1);
    QueryResultKey newKey = new QueryResultKey(query, newFilters, null, 0);

    assertEquals(key, newKey);
  }

}
