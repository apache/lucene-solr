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
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase.StoppableThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StoppableCommitThread extends StoppableThread {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  private final SolrClient cloudClient;
  private final long timeBetweenCommitsMs;
  private final boolean softCommits;
  private volatile boolean stop = false;
  private final AtomicInteger numCommits = new AtomicInteger(0);
  private final AtomicInteger numFails = new AtomicInteger(0);

  public StoppableCommitThread(SolrClient cloudClient, long timeBetweenCommitsMs, boolean softCommits) {
    super("StoppableCommitThread");
    this.cloudClient = cloudClient;
    this.timeBetweenCommitsMs = timeBetweenCommitsMs;
    this.softCommits = softCommits;
  }
  
  @Override
  public void run() {
    log.debug("StoppableCommitThread started");
    while (!stop) {
      try {
        cloudClient.commit(false, false, softCommits);
        numCommits.incrementAndGet();
      } catch (Exception e) {
        numFails.incrementAndGet();
      }
      try {
        Thread.sleep(timeBetweenCommitsMs);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
    if (log.isInfoEnabled()) {
      log.info("StoppableCommitThread finished. Committed {} times. Failed {} times.", numCommits.get(), numFails.get());
    }
  }

  @Override
  public void safeStop() {
    this.stop = true;
  }

}
