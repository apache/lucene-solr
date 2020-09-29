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

import java.io.IOException;

import org.apache.solr.client.solrj.cloud.DistributedQueue;
import org.apache.solr.client.solrj.cloud.DistributedQueueFactory;
import org.apache.solr.client.solrj.cloud.DistribStateManager;

/**
 * Factory for {@link GenericDistributedQueue}.
 *
 * @deprecated to be removed in Solr 9.0 (see SOLR-14656)
 */
public class GenericDistributedQueueFactory implements DistributedQueueFactory {

  private final DistribStateManager stateManager;

  public GenericDistributedQueueFactory(DistribStateManager stateManager) {
    this.stateManager = stateManager;
  }

  @Override
  public DistributedQueue makeQueue(String path) throws IOException {
    return new GenericDistributedQueue(stateManager, path);
  }

  @Override
  public void removeQueue(String path) throws IOException {

  }
}
