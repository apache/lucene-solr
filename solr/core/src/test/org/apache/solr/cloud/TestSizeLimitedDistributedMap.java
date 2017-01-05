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

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.SolrZkClient;

public class TestSizeLimitedDistributedMap extends SolrTestCaseJ4 {

  public void testCleanup() throws Exception {
    String zkDir = createTempDir("TestSizeLimitedDistributedMap").toFile().getAbsolutePath();

    ZkTestServer server = new ZkTestServer(zkDir);
    try {
      server.run();

      AbstractZkTestCase.tryCleanSolrZkNode(server.getZkHost());
      AbstractZkTestCase.makeSolrZkNode(server.getZkHost());

      try (SolrZkClient zkClient = new SolrZkClient(server.getZkAddress(), 10000)) {
        DistributedMap map = Overseer.getCompletedMap(zkClient);
        assertTrue(map instanceof SizeLimitedDistributedMap);
        for (int i = 0; i < Overseer.NUM_RESPONSES_TO_STORE; i++) {
          map.put("xyz_" + i, new byte[0]);
        }

        assertEquals("Number of items do not match", Overseer.NUM_RESPONSES_TO_STORE, map.size());
        // add another to trigger cleanup
        map.put("xyz_10000", new byte[0]);
        assertEquals("Distributed queue was not cleaned up",
            Overseer.NUM_RESPONSES_TO_STORE - (Overseer.NUM_RESPONSES_TO_STORE / 10) + 1, map.size());
        for (int i = Overseer.NUM_RESPONSES_TO_STORE; i >= Overseer.NUM_RESPONSES_TO_STORE / 10; i--) {
          assertTrue(map.contains("xyz_" + i));
        }
        for (int i = Overseer.NUM_RESPONSES_TO_STORE / 10 - 1; i >= 0; i--) {
          assertFalse(map.contains("xyz_" + i));
        }
      }
    } finally {
      server.shutdown();
    }
  }
}
