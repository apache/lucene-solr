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
package org.apache.solr.morphlines.solr;

import org.apache.http.client.HttpClient;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ConcurrentUpdateSolrServer that propagates exceptions up to the submitter of
 * requests on blockUntilFinished()
 */
final class SafeConcurrentUpdateSolrServer extends ConcurrentUpdateSolrServer {

  private Throwable currentException = null;
  private final Object myLock = new Object();

  private static final Logger LOGGER = LoggerFactory.getLogger(SafeConcurrentUpdateSolrServer.class);

  public SafeConcurrentUpdateSolrServer(String solrServerUrl, int queueSize, int threadCount) {
    this(solrServerUrl, null, queueSize, threadCount);
  }

  public SafeConcurrentUpdateSolrServer(String solrServerUrl, HttpClient client, int queueSize, int threadCount) {
    super(solrServerUrl, client, queueSize, threadCount);
  }

  @Override
  public void handleError(Throwable ex) {
    assert ex != null;
    synchronized (myLock) {
      currentException = ex;
    }
    LOGGER.error("handleError", ex);
  }

  @Override
  public void blockUntilFinished() {
    super.blockUntilFinished();
    synchronized (myLock) {
      if (currentException != null) {
        throw new RuntimeException(currentException);
      }
    }
  }

  public void clearException() {
    synchronized (myLock) {
      currentException = null;
    }
  }

}
