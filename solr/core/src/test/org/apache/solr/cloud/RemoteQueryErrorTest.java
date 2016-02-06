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

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.internal.matchers.StringContains.containsString;

/**
 * Verify that remote (proxied) queries return proper error messages
 */
@Slow
public class RemoteQueryErrorTest extends AbstractFullDistribZkTestBase {

  public RemoteQueryErrorTest() {
    super();
    sliceCount = 1;
    fixShardCount(random().nextBoolean() ? 3 : 4);
  }

  @Test
  public void test() throws Exception {
    handle.clear();
    handle.put("timestamp", SKIPVAL);
    
    waitForThingsToLevelOut(15);

    del("*:*");
    
    createCollection("collection2", 2, 1, 10);
    
    List<Integer> numShardsNumReplicaList = new ArrayList<>(2);
    numShardsNumReplicaList.add(2);
    numShardsNumReplicaList.add(1);
    checkForCollection("collection2", numShardsNumReplicaList, null);
    waitForRecoveriesToFinish("collection2", true);

    for (SolrClient solrClient : clients) {
      try {
        SolrInputDocument emptyDoc = new SolrInputDocument();
        solrClient.add(emptyDoc);
        fail("Expected unique key exceptoin");
      } catch (SolrException ex) {
        assertThat(ex.getMessage(), containsString("Document is missing mandatory uniqueKey field: id"));
      } catch(Exception ex) {
        fail("Expected a SolrException to occur, instead received: " + ex.getClass());
      } finally {
        solrClient.close();
      }
    }
  }
}
