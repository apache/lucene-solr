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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrServer;
import org.apache.solr.client.solrj.response.SolrPingResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A vehicle to load a list of Solr documents into a local or remote {@link SolrServer}.
 */
public class SolrServerDocumentLoader implements DocumentLoader {

  private final SolrServer server; // proxy to local or remote solr server
  private long numLoadedDocs = 0; // number of documents loaded in the current transaction
  private final int batchSize;
  private final List<SolrInputDocument> batch = new ArrayList();

  private static final Logger LOGGER = LoggerFactory.getLogger(SolrServerDocumentLoader.class);

  public SolrServerDocumentLoader(SolrServer server, int batchSize) {
    if (server == null) {
      throw new IllegalArgumentException("solr server must not be null");
    }
    this.server = server;
    if (batchSize <= 0) {
      throw new IllegalArgumentException("batchSize must be a positive number: " + batchSize);      
    }
    this.batchSize = batchSize;
  }
  
  @Override
  public void beginTransaction() {
    LOGGER.trace("beginTransaction");
    batch.clear();
    numLoadedDocs = 0;
    if (server instanceof SafeConcurrentUpdateSolrServer) {
      ((SafeConcurrentUpdateSolrServer) server).clearException();
    }
  }

  @Override
  public void load(SolrInputDocument doc) throws IOException, SolrServerException {
    LOGGER.trace("load doc: {}", doc);
    batch.add(doc);
    if (batch.size() >= batchSize) {
      loadBatch();
    }
  }

  @Override
  public void commitTransaction() throws SolrServerException, IOException {
    LOGGER.trace("commitTransaction");
    if (batch.size() > 0) {
      loadBatch();
    }
    if (numLoadedDocs > 0) {
      if (server instanceof ConcurrentUpdateSolrServer) {
        ((ConcurrentUpdateSolrServer) server).blockUntilFinished();
      }
    }
  }

  private void loadBatch() throws SolrServerException, IOException {
    numLoadedDocs += batch.size();
    try {
      UpdateResponse rsp = server.add(batch);
    } finally {
      batch.clear();
    }
  }

  @Override
  public UpdateResponse rollbackTransaction() throws SolrServerException, IOException {
    LOGGER.trace("rollback");
    if (!(server instanceof CloudSolrServer)) {
      return server.rollback();
    } else {
      return new UpdateResponse();
    }
  }

  @Override
  public void shutdown() {
    LOGGER.trace("shutdown");
    server.shutdown();
  }

  @Override
  public SolrPingResponse ping() throws SolrServerException, IOException {
    LOGGER.trace("ping");
    return server.ping();
  }

  public SolrServer getSolrServer() {
    return server;
  }

}
