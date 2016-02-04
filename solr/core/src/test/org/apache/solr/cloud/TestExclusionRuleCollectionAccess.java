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
package org.apache.solr.cloud;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.junit.Test;

@LuceneTestCase.Slow
public class TestExclusionRuleCollectionAccess extends AbstractFullDistribZkTestBase {

  public TestExclusionRuleCollectionAccess() {
    schemaString = "schema15.xml";      // we need a string id
    sliceCount = 1;
  }

  @Test
  public void doTest() throws Exception {
    CollectionAdminRequest.Create req = new CollectionAdminRequest.Create();
    req.setCollectionName("css33");
    req.setNumShards(1);
    req.process(cloudClient);
    
    waitForRecoveriesToFinish("css33", false);
    
    try (SolrClient c = createCloudClient("css33")) {
      c.add(getDoc("id", "1"));
      c.commit();

      assertEquals("Should have returned 1 result", 1, c.query(params("q", "*:*", "collection", "css33")).getResults().getNumFound());
    }
  }
  
}
