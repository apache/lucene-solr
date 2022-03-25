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

import java.util.*;
import java.util.concurrent.*;

import org.apache.solr.common.cloud.SolrZkClient;

public class TestSizeLimitedDistributedMap extends TestDistributedMap {

  public void testCleanup() throws Exception {
    final List<String> deletedItems = new LinkedList<>();
    final Set<String> expectedKeys = new HashSet<>();
    int numResponsesToStore=TEST_NIGHTLY?Overseer.NUM_RESPONSES_TO_STORE:100;
    
    try (SolrZkClient zkClient = new SolrZkClient(zkServer.getZkHost(), 10000)) {
      String path = getAndMakeInitialPath(zkClient);
      DistributedMap map = new SizeLimitedDistributedMap(zkClient, path, numResponsesToStore, (element)->deletedItems.add(element));
      for (int i = 0; i < numResponsesToStore; i++) {
        map.put("xyz_" + i, new byte[0]);
        expectedKeys.add("xyz_" + i);
      }

      assertEquals("Number of items do not match", numResponsesToStore, map.size());
      assertTrue("Expected keys do not match", expectedKeys.containsAll(map.keys()));
      assertTrue("Expected keys do not match", map.keys().containsAll(expectedKeys));
      // add another to trigger cleanup
      map.put("xyz_" + numResponsesToStore, new byte[0]);
      expectedKeys.add("xyz_" + numResponsesToStore);
      assertEquals("Distributed queue was not cleaned up",
          numResponsesToStore - (numResponsesToStore / 10) + 1, map.size());
      for (int i = numResponsesToStore; i >= numResponsesToStore / 10; i--) {
        assertTrue(map.contains("xyz_" + i));
      }
      for (int i = numResponsesToStore / 10 - 1; i >= 0; i--) {
        assertFalse(map.contains("xyz_" + i));
        assertTrue(deletedItems.contains("xyz_" + i));
        expectedKeys.remove("xyz_" + i);
      }
      assertTrue("Expected keys do not match", expectedKeys.containsAll(map.keys()));
      assertTrue("Expected keys do not match", map.keys().containsAll(expectedKeys));
      map.remove("xyz_" + numResponsesToStore);
      assertFalse("map.remove shouldn't trigger the observer", 
          deletedItems.contains("xyz_" + numResponsesToStore));
    }
  }

  public void testConcurrentCleanup() throws Exception {
    final Set<String> expectedKeys = new HashSet<>();
    final List<String> deletedItems = new LinkedList<>();
    int numResponsesToStore=TEST_NIGHTLY?Overseer.NUM_RESPONSES_TO_STORE:100;

    try (SolrZkClient zkClient = new SolrZkClient(zkServer.getZkHost(), 10000)) {
      String path = getAndMakeInitialPath(zkClient);
      DistributedMap map = new SizeLimitedDistributedMap(zkClient, path, numResponsesToStore, (element)->deletedItems.add(element));
      //fill the map to limit first
      for (int i = 0; i < numResponsesToStore; i++) {
        map.put("xyz_" + i, new byte[0]);
      }

      // add more elements concurrently to trigger cleanup
      final int THREAD_COUNT = Math.min(100, numResponsesToStore);
      List<Callable> callables = new ArrayList<>();
      for (int i = 0 ; i < THREAD_COUNT ; i ++) {
        final String key = "xyz_" + (numResponsesToStore + 1);
        expectedKeys.add(key);
        callables.add(() -> {
          map.put(key, new byte[0]);
          return null;
        });
      }
      ExecutorService executorService = Executors.newFixedThreadPool(THREAD_COUNT);
      List<Future> futures = new ArrayList<>();
      for (Callable callable : callables) {
         futures.add(executorService.submit(callable));
      }
      try {
        for (Future future : futures) {
          future.get(); //none of them should throw exception
        }
        for (String expectedKey : expectedKeys) {
          assertTrue(map.contains(expectedKey));
        }
        //there's no guarantees on exactly how many elements will be removed, but it should at least NOT throw exception
        assertTrue(!deletedItems.isEmpty());
      } finally {
        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.SECONDS);
      }
    }
  }
  
  protected DistributedMap createMap(SolrZkClient zkClient, String path) {
    return new SizeLimitedDistributedMap(zkClient, path, Overseer.NUM_RESPONSES_TO_STORE, null);
  }
  
}
