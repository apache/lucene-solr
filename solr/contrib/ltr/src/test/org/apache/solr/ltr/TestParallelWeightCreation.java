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
package org.apache.solr.ltr;

import org.apache.solr.client.solrj.SolrQuery;
import org.junit.Test;

public class TestParallelWeightCreation extends TestRerankBase{

  @Test
  public void testLTRScoringQueryParallelWeightCreationResultOrder() throws Exception {
    setuptest("solrconfig-ltr_Th10_10.xml", "schema.xml");

    assertU(adoc("id", "1", "title", "w1 w3", "description", "w1", "popularity", "1"));
    assertU(adoc("id", "2", "title", "w2",    "description", "w2", "popularity", "2"));
    assertU(adoc("id", "3", "title", "w3",    "description", "w3", "popularity", "3"));
    assertU(adoc("id", "4", "title", "w3 w3", "description", "w4", "popularity", "4"));
    assertU(adoc("id", "5", "title", "w5",    "description", "w5", "popularity", "5"));
    assertU(commit());

    loadFeatures("external_features.json");
    loadModels("external_model.json");

    // check to make sure that the order of results will be the same when using parallel weight creation
    final SolrQuery query = new SolrQuery();
    query.setQuery("*:*");
    query.add("fl", "*,score");
    query.add("rows", "4");

    query.add("rq", "{!ltr reRankDocs=10 model=externalmodel efi.user_query=w3}");
    // SOLR-10710, feature based on query with term w3 now scores higher on doc 4, updated
    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/id=='4'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[1]/id=='3'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[2]/id=='1'");
    aftertest();
  }

  @Test
  public void testLTRQParserThreadInitialization() throws Exception {
    // setting the value of number of threads to -ve should throw an exception
    String msg1 = null;
    try{
      new LTRThreadModule(1,-1);
    }catch(IllegalArgumentException iae){
      msg1 = iae.getMessage();;
    }
    assertTrue(msg1.equals("numThreadsPerRequest cannot be less than 1"));

    // set totalPoolThreads to 1 and numThreadsPerRequest to 2 and verify that an exception is thrown
    String msg2 = null;
    try{
      new LTRThreadModule(1,2);
    }catch(IllegalArgumentException iae){
      msg2 = iae.getMessage();
    }
    assertTrue(msg2.equals("numThreadsPerRequest cannot be greater than totalPoolThreads"));
  }
}
