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

package org.apache.solr.search;

import java.io.IOException;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.BaseHttpSolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

@LuceneTestCase.Slow
public class FuzzySearchTest extends SolrCloudTestCase {
  private final static String COLLECTION = "c1";
  private CloudSolrClient client;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1).addConfig(COLLECTION, configset("cloud-minimal")).configure();
  }

  @Before
  public void setupCollection() throws Exception {
    client = cluster.getSolrClient();
    client.setDefaultCollection(COLLECTION);

    CollectionAdminRequest.createCollection(COLLECTION, 1, 1).process(client);
    cluster.waitForActiveCollection(COLLECTION, 1, 1);
  }

  @Test
  public void testTooComplex() throws IOException, SolrServerException {
    SolrInputDocument doc = new SolrInputDocument();

    doc.setField("id", "1");
    doc.setField("text", "foo");
    client.add(doc);
    client.commit(); // Must have index files written, but the contents don't matter

    SolrQuery query = new SolrQuery("text:headquarters\\(在日米海軍横須賀基地司令部庁舎\\/旧横須賀鎮守府会議所・横須賀海軍艦船部在日米海軍横須賀基地司令部庁舎\\/旧横須賀鎮守府会議所・横須賀海軍艦船部在日米海軍横須賀基地司令部庁舎\\/旧横須賀鎮守府会議所・横須賀海軍艦船部在日米海軍横須賀基地司令部庁舎\\/旧横須賀鎮守府会議所・横須賀海軍艦船部在日米海軍横須賀基地司令部庁舎\\/旧横須賀鎮守府会議所・横須賀海軍艦船部在日米海軍横須賀基地司令部庁舎\\/旧横須賀鎮守府会議所・横須賀海軍艦船部在日米海軍横須賀基地司令部庁舎\\/旧横須賀鎮守府会議所・横須賀海軍艦船部在日米海軍横須賀基地司令部庁舎\\/旧横須賀鎮守府会議所・横須賀海軍艦船部在日米海軍横須賀基地司令部庁舎\\/旧横須賀鎮守府会議所・横須賀海軍艦船部\\)~");

    BaseHttpSolrClient.RemoteSolrException e = expectThrows(BaseHttpSolrClient.RemoteSolrException.class, () -> client.query(query));
    assertTrue("Should be client error, not server error", e.code() >= 400 && e.code() < 500);
  }
}
