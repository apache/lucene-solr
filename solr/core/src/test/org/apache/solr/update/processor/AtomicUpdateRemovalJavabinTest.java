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

package org.apache.solr.update.processor;

import java.time.Instant;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests Solr's atomic-update functionality using requests sent through SolrJ using wt=javabin
 *
 * {@link AtomicUpdatesTest} covers some of the same functionality, but does so by making xml-based requests.  Recent
 * changes to Solr have made it possible for the same data sent with different formats to result in different NamedLists
 * after unmarshalling, so the test duplication is now necessary.  See SOLR-13331 for an example.
 */
public class AtomicUpdateRemovalJavabinTest extends SolrCloudTestCase {
  private static final String COLLECTION = "collection1";
  private static final int NUM_SHARDS = 1;
  private static final int NUM_REPLICAS = 1;
  private static final int MAX_SHARDS_PER_NODE = 1;
  private static final Date DATE_1 = new Date();
  private static final Date DATE_2 = Date.from(Instant.ofEpochSecond(1554243909));

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1)
        .addConfig("conf", configset("cloud-dynamic"))
        .configure();

    CollectionAdminRequest.createCollection(COLLECTION, "conf", NUM_SHARDS, NUM_REPLICAS)
        .setMaxShardsPerNode(MAX_SHARDS_PER_NODE)
        .process(cluster.getSolrClient());

    cluster.waitForActiveCollection(COLLECTION, 1, 1);

    final SolrInputDocument doc1 = sdoc(
        "id", "1",
        "title_s", "title_1", "title_s", "title_2",
        "tv_mv_text", "text_1", "tv_mv_text", "text_2",
        "count_is", 1, "count_is", 2,
        "count_md", 1.0, "count_md", 2.0,
        "timestamps_mdt", DATE_1, "timestamps_mdt", DATE_2);
    final UpdateRequest req = new UpdateRequest()
        .add(doc1);
    req.commit(cluster.getSolrClient(), COLLECTION);
  }

  @Test
  public void testAtomicUpdateRemovalOfStrField() throws Exception {
    ensureFieldHasValues("1", "title_s", "title_1", "title_2");
    atomicRemoveValueFromField("1", "title_s", "title_1");
    ensureFieldHasValues("1", "title_s", "title_2");
  }

  @Test
  public void testAtomicUpdateRemovalOfTextField() throws Exception {
    ensureFieldHasValues("1", "tv_mv_text", "text_1", "text_2");
    atomicRemoveValueFromField("1", "tv_mv_text", "text_1");
    ensureFieldHasValues("1", "tv_mv_text", "text_2");
  }

  @Test
  public void testAtomicUpdateRemovalOfIntField() throws Exception {
    ensureFieldHasValues("1", "count_is", 1, 2);
    atomicRemoveValueFromField("1", "count_is", 1);
    ensureFieldHasValues("1", "count_is", 2);
  }

  @Test
  public void testAtomicUpdateRemovalOfDoubleField() throws Exception {
    ensureFieldHasValues("1", "count_md", 1.0, 2.0);
    atomicRemoveValueFromField("1", "count_md", 1.0);
    ensureFieldHasValues("1", "count_md", 2.0);
  }

  @Test
  public void testAtomicUpdateRemovalOfDateField() throws Exception {
    ensureFieldHasValues("1", "timestamps_mdt", DATE_1, DATE_2);
    atomicRemoveValueFromField("1", "timestamps_mdt", DATE_1);
    ensureFieldHasValues("1", "timestamps_mdt", DATE_2);
  }

  private void atomicRemoveValueFromField(String docId, String fieldName, Object value) throws Exception {
    final SolrInputDocument doc = new SolrInputDocument();
    doc.setField("id", docId);
    Map<String, Object> atomicUpdateRemoval = new HashMap<>(1);
    atomicUpdateRemoval.put("remove", value);
    doc.setField(fieldName, atomicUpdateRemoval);

    cluster.getSolrClient().add(COLLECTION, doc);
    cluster.getSolrClient().commit(COLLECTION);
  }

  private void ensureFieldHasValues(String identifyingDocId, String fieldName, Object... expectedValues) throws Exception {
    final SolrClient client = cluster.getSolrClient();

    final QueryResponse response = client.query(COLLECTION, new SolrQuery("id:" + identifyingDocId));
    final SolrDocumentList docs = response.getResults();
    assertEquals(1, docs.getNumFound());
    final Collection<Object> valuesAfterUpdate = docs.get(0).getFieldValues(fieldName);
    assertEquals(expectedValues.length, valuesAfterUpdate.size());
    for (Object expectedValue: expectedValues) {
      assertTrue("Expected value [" + expectedValue + "] was not found in field", valuesAfterUpdate.contains(expectedValue));
    }
  }
}
