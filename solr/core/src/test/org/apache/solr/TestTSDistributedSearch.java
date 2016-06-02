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

package org.apache.solr;

import org.junit.Test;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.ModifiableSolrParams;

/**
 * Tests uniqueKey Date docs work down to milliseconds
 */
@Slow
public class TestTSDistributedSearch extends BaseDistributedSearchTestCase {

  @Test
  public void test() throws Exception {
    del("*:*");
    indexr(id,"2010-05-02T11:00:00.222Z");
    indexr(id,"2010-05-02T11:00:00.333Z");
    commit();
    
    // We should return 2 docs, millisecond resolution should be preserved
    ModifiableSolrParams q = new ModifiableSolrParams();
    q.set("q", "*:*");
    setDistributedParams(q);
    QueryResponse rsp = queryServer(q);
    assertEquals(2, rsp.getResults().size());
    assertEquals(2, ((SolrDocumentList)rsp.getResponse().get("response")).getNumFound());
    
    // Both distributed and non distributed paths should behave the same
    handle.clear();
    handle.put("maxScore", SKIPVAL);
    handle.put("QTime", SKIPVAL);
    handle.put("timestamp", SKIPVAL);
    handle.put("_version_", SKIPVAL);
    query("q","*:*", "sort", "id asc");
  }
 
  @Override
  protected String getSchemaFileOverride() {
    return "TSid-schema.xml"; 
  }
  
}


