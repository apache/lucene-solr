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

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.cloud.api.collections.OverseerCollectionMessageHandler;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CommonAdminParams;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.Utils;
import org.junit.Test;

public class OverseerTaskQueueTest extends DistributedQueueTest {


  // TODO: OverseerTaskQueue specific tests.

  @Override
  protected OverseerTaskQueue makeDistributedQueue(String dqZNode) throws Exception {
    return new OverseerTaskQueue(zkClient, setupNewDistributedQueueZNode(dqZNode));
  }

  @Test
  public void testContainsTaskWithRequestId() throws Exception {
    String tqZNode = "/taskqueue/test";
    String requestId = "foo";
    String nonExistentRequestId = "bar";

    OverseerTaskQueue tq = makeDistributedQueue(tqZNode);

    // Basic ops
    // Put an expected Overseer task onto the queue
    final Map<String, Object> props = new HashMap<>();
    props.put(CommonParams.NAME, "coll1");
    props.put(CollectionAdminParams.COLL_CONF, "myconf");
    props.put(OverseerCollectionMessageHandler.NUM_SLICES, 1);
    props.put(ZkStateReader.REPLICATION_FACTOR, 3);
    props.put(CommonAdminParams.ASYNC, requestId);
    tq.offer(Utils.toJSON(props));

    assertTrue("Task queue should contain task with requestid " + requestId,
        tq.containsTaskWithRequestId(CommonAdminParams.ASYNC, requestId));

    assertFalse("Task queue should not contain task with requestid " + nonExistentRequestId,
        tq.containsTaskWithRequestId(CommonAdminParams.ASYNC, nonExistentRequestId));

    // Create a response node as if someone is waiting for a response from the Overseer; then,
    // create the request node.
    // Here we're reaching a bit into the internals of OverseerTaskQueue in order to create the same
    // response node structure but without setting a watch on it and removing it immediately when
    // a response is set, in order to artificially create the race condition that
    // containsTaskWithRequestId runs while the response is still in the queue.
    String watchID = tq.createResponseNode();
    String requestId2 = "baz";
    props.put(CommonAdminParams.ASYNC, requestId2);
    tq.createRequestNode(Utils.toJSON(props), watchID);

    // Set a SolrResponse as the response node by removing the QueueEvent, as done in OverseerTaskProcessor
    List<OverseerTaskQueue.QueueEvent> queueEvents = tq.peekTopN(2, s -> false, 1000);
    OverseerTaskQueue.QueueEvent requestId2Event = null;
    for (OverseerTaskQueue.QueueEvent queueEvent : queueEvents) {
      @SuppressWarnings({"unchecked"})
      Map<String, Object> eventProps = (Map<String, Object>) Utils.fromJSON(queueEvent.getBytes());
      if (requestId2.equals(eventProps.get(CommonAdminParams.ASYNC))) {
        requestId2Event = queueEvent;
        break;
      }
    }
    assertNotNull("Didn't find event with requestid " + requestId2, requestId2Event);
    requestId2Event.setBytes("foo bar".getBytes(StandardCharsets.UTF_8));
    tq.remove(requestId2Event);

    // Make sure this call to check if requestId exists doesn't barf with Json parse exception
    assertTrue("Task queue should contain task with requestid " + requestId,
        tq.containsTaskWithRequestId(CommonAdminParams.ASYNC, requestId));
  }
}
