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

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class StoppableSearchThread extends AbstractFullDistribZkTestBase.StoppableThread {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final CloudSolrClient cloudClient;
  private volatile boolean stop = false;
  protected final AtomicInteger queryFails = new AtomicInteger();
  private String[] QUERIES = new String[] {"to come","their country","aid","co*"};

  public StoppableSearchThread(CloudSolrClient cloudClient) {
    super("StoppableSearchThread");
    this.cloudClient = cloudClient;
    setDaemon(true);
  }

  @Override
  public void run() {
    Random random = LuceneTestCase.random();
    int numSearches = 0;

    while (!stop) {
      numSearches++;
      try {
        //to come to the aid of their country.
        cloudClient.query(new SolrQuery(QUERIES[random.nextInt(QUERIES.length)]));
      } catch (Exception e) {
        System.err.println("QUERY REQUEST FAILED:");
        e.printStackTrace();
        if (e instanceof SolrServerException) {
          System.err.println("ROOT CAUSE:");
          ((SolrServerException) e).getRootCause().printStackTrace();
        }
        queryFails.incrementAndGet();
      }
      try {
        Thread.sleep(random.nextInt(4000) + 300);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    log.info("num searches done: {} with {} fails", numSearches, queryFails);
  }

  @Override
  public void safeStop() {
    stop = true;
  }

  public int getFails() {
    return queryFails.get();
  }

}
