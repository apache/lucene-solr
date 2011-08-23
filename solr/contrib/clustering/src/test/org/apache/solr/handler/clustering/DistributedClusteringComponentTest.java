package org.apache.solr.handler.clustering;

/**
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

import org.apache.solr.BaseDistributedSearchTestCase;
import org.apache.solr.common.params.CommonParams;

public class DistributedClusteringComponentTest extends
    BaseDistributedSearchTestCase {

  @Override
  public String getSolrHome() {
    return "clustering/solr";
  }

  @Override
  public void doTest() throws Exception {
    del("*:*");
    int numberOfDocs = 0;
    for (String[] doc : AbstractClusteringTestCase.DOCUMENTS) {
      index(id, Integer.toString(numberOfDocs++), "url", doc[0], "title", doc[1], "snippet", doc[2]);
    }
    commit();
    handle.clear();
    // Only really care about the clusters for this test case, so drop the header and response
    handle.put("responseHeader", SKIP);
    handle.put("response", SKIP);
    query(                                                                                                   
        ClusteringComponent.COMPONENT_NAME, "true",
        CommonParams.Q, "*:*",
        CommonParams.SORT, id + " desc",
        ClusteringParams.USE_SEARCH_RESULTS, "true");
    // destroy is not needed because tearDown method of base class does it.
    //destroyServers();
  }

}
