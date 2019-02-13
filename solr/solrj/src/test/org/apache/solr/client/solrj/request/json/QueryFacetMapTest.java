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

package org.apache.solr.client.solrj.request.json;

import org.apache.solr.SolrTestCaseJ4;
import org.junit.Test;

import static org.hamcrest.core.StringContains.containsString;

public class QueryFacetMapTest extends SolrTestCaseJ4 {
  @Test
  public void testSetsFacetTypeToQuery() {
    final QueryFacetMap queryFacet = new QueryFacetMap("any:query");
    assertEquals("query", queryFacet.get("type"));
  }

  @Test
  public void testRejectsInvalidQueryString() {
    final Throwable thrown = expectThrows(IllegalArgumentException.class, () -> {
      final QueryFacetMap queryFacet = new QueryFacetMap(null);
    });
    assertThat(thrown.getMessage(), containsString("must be non-null"));
  }

  @Test
  public void testSetsQueryWithCorrectKey() {
    final QueryFacetMap queryFacet = new QueryFacetMap("any:query");
    assertEquals("any:query", queryFacet.get("q"));
  }
}
