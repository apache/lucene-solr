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
  private static final String COMMITTED_DOC_ID = "1";
  private static final String UNCOMMITTED_DOC_ID = "2";
  private static final String COLLECTION = "collection1";
  private static final int NUM_SHARDS = 1;
  private static final int NUM_REPLICAS = 1;
  private static final Date DATE_1 = new Date();
  private static final Date DATE_2 = Date.from(Instant.ofEpochSecond(1554243909));

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1)
        .addConfig("conf", configset("cloud-dynamic"))
        .configure();

    CollectionAdminRequest.createCollection(COLLECTION, "conf", NUM_SHARDS, NUM_REPLICAS)
        .process(cluster.getSolrClient());

    cluster.waitForActiveCollection(COLLECTION, 1, 1);

    final SolrInputDocument committedDoc = sdoc(
        "id", COMMITTED_DOC_ID,
        "title_s", "title_1", "title_s", "title_2",
        "tv_mv_text", "text_1", "tv_mv_text", "text_2",
        "count_is", 1, "count_is", 2,
        "count_md", 1.0, "count_md", 2.0,
        "timestamps_mdt", DATE_1, "timestamps_mdt", DATE_2);
    final UpdateRequest committedRequest = new UpdateRequest()
        .add(committedDoc);
    committedRequest.commit(cluster.getSolrClient(), COLLECTION);

    // Upload a copy of id:1 that's uncommitted to test how atomic-updates modify values in the tlog
    // See SOLR-14971 for an example of why this case needs tested separately
    final SolrInputDocument uncommittedDoc = sdoc(
            "id", UNCOMMITTED_DOC_ID,
            "title_s", "title_1", "title_s", "title_2",
            "tv_mv_text", "text_1", "tv_mv_text", "text_2",
            "count_is", 1, "count_is", 2,
            "count_md", 1.0, "count_md", 2.0,
            "timestamps_mdt", DATE_1, "timestamps_mdt", DATE_2);
    final UpdateRequest uncommittedRequest = new UpdateRequest()
            .add(uncommittedDoc);
    uncommittedRequest.process(cluster.getSolrClient(), COLLECTION);
  }

  @Test
  public void testAtomicUpdateRemovalOfStrField() throws Exception {
    ensureFieldHasValues(COMMITTED_DOC_ID, "title_s", "title_1", "title_2");
    atomicRemoveValueFromField(COMMITTED_DOC_ID, "title_s", "title_1");
    ensureFieldHasValues(COMMITTED_DOC_ID, "title_s", "title_2");

    ensureFieldHasValues(UNCOMMITTED_DOC_ID, "title_s", "title_1", "title_2");
    atomicRemoveValueFromField(UNCOMMITTED_DOC_ID, "title_s", "title_1");
    ensureFieldHasValues(UNCOMMITTED_DOC_ID, "title_s", "title_2");
  }

  @Test
  public void testAtomicUpdateRemovalOfTextField() throws Exception {
    ensureFieldHasValues(COMMITTED_DOC_ID, "tv_mv_text", "text_1", "text_2");
    atomicRemoveValueFromField(COMMITTED_DOC_ID, "tv_mv_text", "text_1");
    ensureFieldHasValues(COMMITTED_DOC_ID, "tv_mv_text", "text_2");

    ensureFieldHasValues(UNCOMMITTED_DOC_ID, "tv_mv_text", "text_1", "text_2");
    atomicRemoveValueFromField(UNCOMMITTED_DOC_ID, "tv_mv_text", "text_1");
    ensureFieldHasValues(UNCOMMITTED_DOC_ID, "tv_mv_text", "text_2");
  }

  @Test
  public void testAtomicUpdateRemovalOfIntField() throws Exception {
    ensureFieldHasValues(COMMITTED_DOC_ID, "count_is", 1, 2);
    atomicRemoveValueFromField(COMMITTED_DOC_ID, "count_is", 1);
    ensureFieldHasValues(COMMITTED_DOC_ID, "count_is", 2);

    ensureFieldHasValues(UNCOMMITTED_DOC_ID, "count_is", 1, 2);
    atomicRemoveValueFromField(UNCOMMITTED_DOC_ID, "count_is", 1);
    ensureFieldHasValues(UNCOMMITTED_DOC_ID, "count_is", 2);
  }

  @Test
  public void testAtomicUpdateRemovalOfDoubleField() throws Exception {
    ensureFieldHasValues(COMMITTED_DOC_ID, "count_md", 1.0, 2.0);
    atomicRemoveValueFromField(COMMITTED_DOC_ID, "count_md", 1.0);
    ensureFieldHasValues(COMMITTED_DOC_ID, "count_md", 2.0);

    ensureFieldHasValues(UNCOMMITTED_DOC_ID, "count_md", 1.0, 2.0);
    atomicRemoveValueFromField(UNCOMMITTED_DOC_ID, "count_md", 1.0);
    ensureFieldHasValues(UNCOMMITTED_DOC_ID, "count_md", 2.0);
  }

  @Test
  public void testAtomicUpdateRemovalOfDateField() throws Exception {
    ensureFieldHasValues(COMMITTED_DOC_ID, "timestamps_mdt", DATE_1, DATE_2);
    atomicRemoveValueFromField(COMMITTED_DOC_ID, "timestamps_mdt", DATE_1);
    ensureFieldHasValues(COMMITTED_DOC_ID, "timestamps_mdt", DATE_2);

    ensureFieldHasValues(UNCOMMITTED_DOC_ID, "timestamps_mdt", DATE_1, DATE_2);
    atomicRemoveValueFromField(UNCOMMITTED_DOC_ID, "timestamps_mdt", DATE_1);
    ensureFieldHasValues(UNCOMMITTED_DOC_ID, "timestamps_mdt", DATE_2);
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
