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

import java.util.ArrayList;
import java.util.List;

import org.apache.solr.client.solrj.response.SolrPingResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A mockup DocumentLoader implementation for unit tests; collects all documents into a main memory list.
 */
class CollectingDocumentLoader implements DocumentLoader {

  private final int batchSize;
  private final List<SolrInputDocument> batch = new ArrayList<SolrInputDocument> ();
  private List<SolrInputDocument> results = new ArrayList<SolrInputDocument> ();

  private static final Logger LOGGER = LoggerFactory.getLogger(CollectingDocumentLoader.class);

  public CollectingDocumentLoader(int batchSize) {
    if (batchSize <= 0) {
      throw new IllegalArgumentException("batchSize must be a positive number: " + batchSize);      
    }
    this.batchSize = batchSize;
  }
  
  @Override
  public void beginTransaction() {
    LOGGER.trace("beginTransaction");
    batch.clear();
  }

  @Override
  public void load(SolrInputDocument doc) {
    LOGGER.trace("load doc: {}", doc);
    batch.add(doc);
    if (batch.size() >= batchSize) {
      loadBatch();
    }
  }

  @Override
  public void commitTransaction() {
    LOGGER.trace("commitTransaction");
    if (batch.size() > 0) {
      loadBatch();
    }
  }

  private void loadBatch() {
    try {
      results.addAll(batch);
    } finally {
      batch.clear();
    }
  }

  @Override
  public UpdateResponse rollbackTransaction() {
    LOGGER.trace("rollback");
    return new UpdateResponse();
  }

  @Override
  public void shutdown() {
    LOGGER.trace("shutdown");    
  }

  @Override
  public SolrPingResponse ping() {
    LOGGER.trace("ping");
    return new SolrPingResponse();
  }

}
