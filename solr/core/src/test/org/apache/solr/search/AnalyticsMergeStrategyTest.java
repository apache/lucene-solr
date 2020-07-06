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

import org.apache.solr.BaseDistributedSearchTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.junit.BeforeClass;
import org.junit.Test;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope.Scope;

/**
 * Test for QueryComponent's distributed querying
 *
 * @see org.apache.solr.handler.component.QueryComponent
 */

@SolrTestCaseJ4.SuppressSSL(bugUrl="https://issues.apache.org/jira/browse/SOLR-8433")
@ThreadLeakScope(Scope.NONE)
public class AnalyticsMergeStrategyTest extends BaseDistributedSearchTestCase {


  public AnalyticsMergeStrategyTest() {
    stress = 0;
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    initCore("solrconfig-analytics-query.xml", "schema15.xml");
  }

  @Test
  @ShardsFixed(num = 3)
  public void test() throws Exception {
    del("*:*");

    index_specific(0,"id","1", "sort_i", "5");
    index_specific(0,"id","2", "sort_i", "50");
    index_specific(1,"id","5", "sort_i", "4");
    index_specific(1,"id","6", "sort_i", "10");
    index_specific(0,"id","7", "sort_i", "1");
    index_specific(1,"id","8", "sort_i", "2");
    index_specific(2,"id","9", "sort_i", "1000");
    index_specific(2,"id","10", "sort_i", "1500");
    index_specific(2,"id","11", "sort_i", "1300");
    index_specific(1,"id","12", "sort_i", "15");
    index_specific(1,"id","13", "sort_i", "16");

    commit();

    /*
    *  The count qparser plugin is pointing to AnalyticsTestQParserPlugin. This class defines a simple AnalyticsQuery and
    *  has two merge strategies. If the iterate local param is true then an InterativeMergeStrategy is used.
    */


    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!count}");
    setDistributedParams(params);
    QueryResponse rsp = queryServer(params);
    assertCount(rsp, 11);

    //Test IterativeMergeStrategy
    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!count iterate=true}");
    setDistributedParams(params);
    rsp = queryServer(params);
    assertCountOnly(rsp, 44);

    params = new ModifiableSolrParams();
    params.add("q", "id:(1 2 5 6)");
    params.add("fq", "{!count}");
    setDistributedParams(params);
    rsp = queryServer(params);
    assertCount(rsp, 4);
  }

  private void assertCountOnly(QueryResponse rsp, int count) throws Exception {
    @SuppressWarnings({"rawtypes"})
    NamedList response = rsp.getResponse();
    @SuppressWarnings({"rawtypes"})
    NamedList analytics = (NamedList)response.get("analytics");
    Integer c = (Integer)analytics.get("mycount");
    if(c.intValue() != count) {
      throw new Exception("Count is not correct:"+count+":"+c.intValue());
    }
  }

  private void assertCount(QueryResponse rsp, int count) throws Exception {
    @SuppressWarnings({"rawtypes"})
    NamedList response = rsp.getResponse();
    @SuppressWarnings({"rawtypes"})
    NamedList analytics = (NamedList)response.get("analytics");
    Integer c = (Integer)analytics.get("mycount");
    if(c.intValue() != count) {
      throw new Exception("Count is not correct:"+count+":"+c.intValue());
    }

    long numFound = rsp.getResults().getNumFound();
    if(c.intValue() != numFound) {
      throw new Exception("Count does not equal numFound:"+c.intValue()+":"+numFound);
    }
  }
}
