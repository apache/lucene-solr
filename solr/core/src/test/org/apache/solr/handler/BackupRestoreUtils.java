package org.apache.solr.handler;

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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BackupRestoreUtils extends LuceneTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static int indexDocs(SolrClient masterClient, String collectionName, long docsSeed) throws IOException, SolrServerException {
    masterClient.deleteByQuery(collectionName, "*:*");

    Random random = new Random(docsSeed);// use a constant seed for the whole test run so that we can easily re-index.
    int nDocs = TestUtil.nextInt(random, 1, 100);
    log.info("Indexing {} test docs", nDocs);

    List<SolrInputDocument> docs = new ArrayList<>(nDocs);
    for (int i = 0; i < nDocs; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", i);
      doc.addField("name", "name = " + i);
      docs.add(doc);
    }
    masterClient.add(collectionName, docs);
    masterClient.commit(collectionName);
    return nDocs;
  }

  public static void verifyDocs(int nDocs, SolrClient masterClient, String collectionName) throws SolrServerException, IOException {
    ModifiableSolrParams queryParams = new ModifiableSolrParams();
    queryParams.set("q", "*:*");
    QueryResponse response = masterClient.query(collectionName, queryParams);

    assertEquals(0, response.getStatus());
    assertEquals(nDocs, response.getResults().getNumFound());
  }
}
