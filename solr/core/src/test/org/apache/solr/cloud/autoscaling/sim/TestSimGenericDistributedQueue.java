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
package org.apache.solr.cloud.autoscaling.sim;

import org.apache.solr.client.solrj.cloud.DistributedQueue;
import org.apache.solr.client.solrj.cloud.DistribStateManager;

/**
 *
 */
public class TestSimGenericDistributedQueue extends TestSimDistributedQueue {
  DistribStateManager stateManager = new SimDistribStateManager();

  @Override
  protected DistributedQueue makeDistributedQueue(String dqZNode) throws Exception {
    return new GenericDistributedQueue(stateManager, dqZNode);
  }

  // commented 4-Sep-2018 @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // added 09-Aug-2018
  @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 14-Oct-2018
  public void testDistributedQueue() throws Exception {
    super.testDistributedQueue();
  }
}
