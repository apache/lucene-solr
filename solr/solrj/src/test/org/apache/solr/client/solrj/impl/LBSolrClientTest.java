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

package org.apache.solr.client.solrj.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LBSolrClientTest {

  @Test
  public void testServerIterator() throws SolrServerException {
    LBSolrClient.Req req = new LBSolrClient.Req(new QueryRequest(), Arrays.asList("1", "2", "3", "4"));
    LBSolrClient.ServerIterator serverIterator = new LBSolrClient.ServerIterator(req, new HashMap<>());
    List<String> actualServers = new ArrayList<>();
    while (serverIterator.hasNext()) {
      actualServers.add(serverIterator.nextOrError());
    }
    assertEquals(Arrays.asList("1", "2", "3", "4"), actualServers);
    assertFalse(serverIterator.hasNext());
    LuceneTestCase.expectThrows(SolrServerException.class, serverIterator::nextOrError);
  }

  @Test
  public void testServerIteratorWithZombieServers() throws SolrServerException {
    HashMap<String, LBSolrClient.ServerWrapper> zombieServers = new HashMap<>();
    LBSolrClient.Req req = new LBSolrClient.Req(new QueryRequest(), Arrays.asList("1", "2", "3", "4"));
    LBSolrClient.ServerIterator serverIterator = new LBSolrClient.ServerIterator(req, zombieServers);
    zombieServers.put("2", new LBSolrClient.ServerWrapper("2"));

    assertTrue(serverIterator.hasNext());
    assertEquals("1", serverIterator.nextOrError());
    assertTrue(serverIterator.hasNext());
    assertEquals("3", serverIterator.nextOrError());
    assertTrue(serverIterator.hasNext());
    assertEquals("4", serverIterator.nextOrError());
    assertTrue(serverIterator.hasNext());
    assertEquals("2", serverIterator.nextOrError());
  }

  @Test
  public void testServerIteratorTimeAllowed() throws SolrServerException, InterruptedException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(CommonParams.TIME_ALLOWED, 300);
    LBSolrClient.Req req = new LBSolrClient.Req(new QueryRequest(params), Arrays.asList("1", "2", "3", "4"), 2);
    LBSolrClient.ServerIterator serverIterator = new LBSolrClient.ServerIterator(req, new HashMap<>());
    assertTrue(serverIterator.hasNext());
    serverIterator.nextOrError();
    Thread.sleep(300);
    LuceneTestCase.expectThrows(SolrServerException.class, serverIterator::nextOrError);
  }

  @Test
  public void testServerIteratorMaxRetry() throws SolrServerException {
    LBSolrClient.Req req = new LBSolrClient.Req(new QueryRequest(), Arrays.asList("1", "2", "3", "4"), 2);
    LBSolrClient.ServerIterator serverIterator = new LBSolrClient.ServerIterator(req, new HashMap<>());
    assertTrue(serverIterator.hasNext());
    serverIterator.nextOrError();
    assertTrue(serverIterator.hasNext());
    serverIterator.nextOrError();
    LuceneTestCase.expectThrows(SolrServerException.class, serverIterator::nextOrError);
  }
}
