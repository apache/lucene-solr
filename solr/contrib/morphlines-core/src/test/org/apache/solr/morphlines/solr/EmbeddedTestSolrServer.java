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

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.core.CoreContainer;

import java.io.IOException;

/**
 * An EmbeddedSolrServer that supresses close and rollback requests as
 * necessary for testing
 */
public class EmbeddedTestSolrServer extends EmbeddedSolrServer {

  public EmbeddedTestSolrServer(CoreContainer coreContainer, String coreName) {
    super(coreContainer, coreName);
  }

  @Override
  public void close() {
    ; // NOP
  }

  @Override
  public UpdateResponse rollback() throws SolrServerException, IOException {
    return new UpdateResponse();
  }

}
