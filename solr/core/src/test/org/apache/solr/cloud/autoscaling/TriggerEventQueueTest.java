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
package org.apache.solr.cloud.autoscaling;

import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.cloud.DistribStateManager;
import org.apache.solr.client.solrj.cloud.DistributedQueueFactory;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.Suggester;
import org.apache.solr.cloud.autoscaling.sim.GenericDistributedQueueFactory;
import org.apache.solr.cloud.autoscaling.sim.SimDistribStateManager;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.util.Pair;
import org.apache.solr.common.util.TimeSource;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 *
 */
public class TriggerEventQueueTest extends SolrTestCaseJ4 {

  SolrCloudManager cloudManager;

  @Before
  public void init() throws Exception {
    assumeWorkingMockito();
    cloudManager = mock(SolrCloudManager.class);
    DistribStateManager stateManager = new SimDistribStateManager();
    when(cloudManager.getDistribStateManager()).thenReturn(stateManager);
    DistributedQueueFactory queueFactory = new GenericDistributedQueueFactory(stateManager);
    when(cloudManager.getDistributedQueueFactory()).thenReturn(queueFactory);
    when(cloudManager.getTimeSource()).thenReturn(TimeSource.NANO_TIME);
  }

  @Test
  @SuppressWarnings({"unchecked"})
  public void testSerialization() throws Exception {
    TriggerEventQueue queue = new TriggerEventQueue(cloudManager, "test", null);
    Map<String, Number> hotHosts = new HashMap<>();
    hotHosts.put("host1", 1);
    hotHosts.put("host2", 1);
    TriggerEvent ev = new MetricTrigger.MetricBreachedEvent("testTrigger", "testCollection", "shard1",
        CollectionParams.CollectionAction.ADDREPLICA.toLower(), cloudManager.getTimeSource().getTimeNs(),
        "foo", hotHosts);
    queue.offerEvent(ev);
    ev = queue.pollEvent();
    assertNotNull(ev);
    Object ops = ev.getProperties().get(TriggerEvent.REQUESTED_OPS);
    assertNotNull(ops);
    assertTrue(ops.getClass().getName(), ops instanceof List);
    List<Object> requestedOps = (List<Object>)ops;
    assertEquals(requestedOps.toString(), 2, requestedOps.size());
    requestedOps.forEach(op -> {
      assertTrue(op.getClass().getName(), op instanceof TriggerEvent.Op);
      TriggerEvent.Op operation = (TriggerEvent.Op)op;
      assertEquals(op.toString(), CollectionParams.CollectionAction.ADDREPLICA, operation.getAction());
      EnumMap<Suggester.Hint, Object> hints = ((TriggerEvent.Op) op).getHints();
      assertEquals(hints.toString(), 2, hints.size());
      Object o = hints.get(Suggester.Hint.COLL_SHARD);
      assertNotNull(Suggester.Hint.COLL_SHARD.toString(), o);
      assertTrue(o.getClass().getName(), o instanceof Collection);
      Collection<Object> col = (Collection<Object>)o;
      assertEquals(col.toString(), 1, col.size());
      o = col.iterator().next();
      assertTrue(o.getClass().getName(), o instanceof Pair);
      o = hints.get(Suggester.Hint.SRC_NODE);
      assertNotNull(Suggester.Hint.SRC_NODE.toString(), o);
      assertTrue(o.getClass().getName(), o instanceof Collection);
      col = (Collection<Object>)o;
      assertEquals(col.toString(), 1, col.size());
      o = col.iterator().next();
      assertTrue(o.getClass().getName(), o instanceof String);
    });
  }
}
