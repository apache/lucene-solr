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

import java.lang.invoke.MethodHandles;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.solr.client.solrj.cloud.ShardTerms;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrCore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Start recovery of a core if its term is less than leader's term
 */
public class RecoveringCoreTermWatcher implements ZkShardTerms.CoreTermWatcher {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final CoreDescriptor coreDescriptor;
  private final CoreContainer coreContainer;
  // used to prevent the case when term of other replicas get changed, we redo recovery
  // the idea here is with a specific term of a replica, we only do recovery one
  private final AtomicLong lastTermDoRecovery;

  RecoveringCoreTermWatcher(CoreDescriptor coreDescriptor, CoreContainer coreContainer) {
    this.coreDescriptor = coreDescriptor;
    this.coreContainer = coreContainer;
    this.lastTermDoRecovery = new AtomicLong(-1);
  }

  @Override
  public boolean onTermChanged(ShardTerms terms) {
    if (coreContainer.isShutDown()) return false;

    try (SolrCore solrCore = coreContainer.getCore(coreDescriptor.getName())) {
      if (solrCore == null || solrCore.isClosed()) {
        return false;
      }

      if (solrCore.getCoreDescriptor() == null || solrCore.getCoreDescriptor().getCloudDescriptor() == null) return true;
      String coreNodeName = solrCore.getCoreDescriptor().getCloudDescriptor().getCoreNodeName();
      if (terms.haveHighestTermValue(coreNodeName)) return true;
      if (lastTermDoRecovery.get() < terms.getTerm(coreNodeName)) {
        log.info("Start recovery on {} because core's term is less than leader's term", coreNodeName);
        lastTermDoRecovery.set(terms.getTerm(coreNodeName));
        solrCore.getUpdateHandler().getSolrCoreState().doRecovery(solrCore.getCoreContainer(), solrCore.getCoreDescriptor());
      }
    } catch (Exception e) {
      log.info("Failed to watch term of core {}", coreDescriptor.getName(), e);
      return false;
    }

    return true;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    RecoveringCoreTermWatcher that = (RecoveringCoreTermWatcher) o;

    return coreDescriptor.getName().equals(that.coreDescriptor.getName());
  }

  @Override
  public int hashCode() {
    return coreDescriptor.getName().hashCode();
  }
}
