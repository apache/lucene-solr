package org.apache.solr.cloud;

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


import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
* Distributed test for {@link org.apache.lucene.index.ExitableDirectoryReader} 
*/
@Slow
public class CloudExitableDirectoryReaderTest extends AbstractFullDistribZkTestBase {
  public static Logger log = LoggerFactory.getLogger(CloudExitableDirectoryReaderTest.class);
  private static final int NUM_DOCS_PER_TYPE = 20;
  
  public CloudExitableDirectoryReaderTest() {
    configString = "solrconfig-tlog-with-delayingcomponent.xml";
    schemaString = "schema.xml";
  }

  @Override
  protected String getCloudSolrConfig() {
    return configString;
  }

  @Override
  public void doTest() throws Exception {
    handle.clear();
    handle.put("timestamp", SKIPVAL);
    waitForRecoveriesToFinish(false);
    indexDocs();
    doTimeoutTests();
  }

  public void indexDocs() throws Exception {
    int counter = 1;

    for(; (counter % NUM_DOCS_PER_TYPE) != 0; counter++ )
      indexDoc(sdoc("id", Integer.toString(counter), "name", "a" + counter));

    counter++;
    for(; (counter % NUM_DOCS_PER_TYPE) != 0; counter++ )
      indexDoc(sdoc("id", Integer.toString(counter), "name", "b" + counter));

    counter++;
    for(; counter % NUM_DOCS_PER_TYPE != 0; counter++ )
      indexDoc(sdoc("id", Integer.toString(counter), "name", "dummy term doc" + counter));

    commit();
  }

  public void doTimeoutTests() throws Exception {
    assertPartialResults(params("q", "name:a*", "timeAllowed", "1"));

    /*
    query rewriting for NUM_DOCS_PER_TYPE terms should take less 
    time than this. Keeping it at 5 because the delaying search component delays all requests 
    by at 1 second.
     */
    long fiveSeconds = 5000L;
    
    Long timeAllowed = TestUtil.nextLong(random(), fiveSeconds, Long.MAX_VALUE);
    assertSuccess(params("q", "name:a*", "timeAllowed",timeAllowed.toString()));

    assertPartialResults(params("q", "name:a*", "timeAllowed", "1"));

    timeAllowed = TestUtil.nextLong(random(), fiveSeconds, Long.MAX_VALUE);
    assertSuccess(params("q", "name:b*", "timeAllowed",timeAllowed.toString()));

    timeAllowed = TestUtil.nextLong(random(), Long.MIN_VALUE, -1L);  // negative timeAllowed should disable timeouts
    assertSuccess(params("q", "name:b*", "timeAllowed",timeAllowed.toString()));

    assertSuccess(params("q","name:b*")); // no time limitation
  }

  /**
   * execute a request, verify that we get an expected error
   */
  public void assertPartialResults(ModifiableSolrParams p) throws Exception {
      QueryResponse rsp = queryServer(p);
      assertEquals("partialResults were expected", true, rsp.getHeader().get("partialResults"));
  }
  
  public void assertSuccess(ModifiableSolrParams p) throws Exception {
    QueryResponse response = queryServer(p);
    assertEquals("Wrong #docs in response", NUM_DOCS_PER_TYPE - 1, response.getResults().getNumFound());
  }
}

