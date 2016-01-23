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
package org.apache.solr.hadoop;

import org.apache.solr.client.solrj.response.SolrPingResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.morphlines.solr.DocumentLoader;

/**
 * Prints documents to stdout instead of loading them into Solr for quicker turnaround during early
 * trial & debug sessions.
 */
final class DryRunDocumentLoader implements DocumentLoader {

  @Override
  public void beginTransaction() {
  }

  @Override
  public void load(SolrInputDocument doc) {
    System.out.println("dryrun: " + doc);
  }

  @Override
  public void commitTransaction() {
  }

  @Override
  public UpdateResponse rollbackTransaction() {
    return new UpdateResponse();
  }

  @Override
  public void shutdown() {
  }

  @Override
  public SolrPingResponse ping() {
    return new SolrPingResponse();
  }

}
