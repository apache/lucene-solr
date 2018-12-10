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
package org.apache.solr.index;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.schema.SchemaResponse;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.FieldType;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.servlet.SolrDispatchFilter;
import org.apache.solr.util.RefCounted;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
@LuceneTestCase.SuppressCodecs({"Memory", "Direct"})
public class ConcurrentIndexUpgradeTest extends AbstractFullDistribZkTestBase {
  private static final Logger log = LoggerFactory.getLogger(ConcurrentIndexUpgradeTest.class);

  private static String ID_FIELD = "id";
  private static String TEST_FIELD = "string_add_dv_later";
  private static final int NUM_DOCS = 1000;

  private AtomicBoolean runIndexer = new AtomicBoolean(true);

  public ConcurrentIndexUpgradeTest() {
    schemaString = "schema-docValues.xml";
  }

  @BeforeClass
  public static void setupTest() {
    System.setProperty("solr.directoryFactory", "solr.StandardDirectoryFactory");
  }

  @AfterClass
  public static void teardownTest() {
    System.clearProperty("solr.directoryFactory");
  }

  @Override
  protected String getCloudSolrConfig() {
    return "solrconfig-pluggablemergepolicyfactory.xml";
  }


  @After
  public void afterTest() throws Exception {
    runIndexer.set(false);
  }

  @Test
  public void testConcurrentIndexUpgrade() throws Exception {
    String collectionName = "concurrentUpgrade_test";
    CollectionAdminRequest.Create createCollectionRequest = CollectionAdminRequest
        .createCollection(collectionName, "conf1", 2, 1);
    CollectionAdminResponse response = createCollectionRequest.process(cloudClient);
    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());
    Thread.sleep(5000);

    cloudClient.setDefaultCollection(collectionName);

    Thread indexerThread = new Thread() {
      @Override
      public void run() {
        try {
          int docId = 0;
          while (runIndexer.get()) {
            UpdateRequest ureq = new UpdateRequest();
            for (int i = 0; i < NUM_DOCS; i++) {
              if (!runIndexer.get()) {
                return;
              }
              SolrInputDocument doc = new SolrInputDocument();
              doc.addField(ID_FIELD, docId);
              doc.addField(TEST_FIELD, String.valueOf(docId));
              ureq.add(doc);
              docId++;
            }
            ureq.process(cloudClient, collectionName);
            cloudClient.commit(collectionName);
            Thread.sleep(200);
          }
        } catch (Exception e) {
          log.warn("Can't index documents", e);
        }
      }
    };

    indexerThread.start();
    // make sure we've indexed some documents
    Thread.sleep(5000);

    CollectionAdminRequest<CollectionAdminRequest.ColStatus> status = new CollectionAdminRequest.ColStatus()
        .setCollectionName(collectionName)
        .setWithFieldInfos(true)
        .setWithSegments(true);
    CollectionAdminResponse rsp = status.process(cloudClient);
    List<String> nonCompliant = (List<String>)rsp.getResponse().findRecursive(collectionName, "schemaNonCompliant");
    assertNotNull("nonCompliant missing: " + rsp, nonCompliant);
    assertEquals("nonCompliant: " + nonCompliant, 1, nonCompliant.size());
    assertEquals("nonCompliant: " + nonCompliant, "(NONE)", nonCompliant.get(0));

    // set plugin configuration
    Map<String, Object> pluginProps = new HashMap<>();
    pluginProps.put(FieldType.CLASS_NAME, AddDocValuesMergePolicyFactory.class.getName());
    // prevent merging
    pluginProps.put(AddDocValuesMergePolicyFactory.NO_MERGE_PROP, true);
    String propValue = Utils.toJSONString(pluginProps);
    CollectionAdminRequest.ClusterProp clusterProp = new CollectionAdminRequest.ClusterProp()
        .setPropertyName(PluggableMergePolicyFactory.MERGE_POLICY_PROP + collectionName)
        .setPropertyValue(propValue);
    clusterProp.process(cloudClient);

    log.info("-- completed set cluster props");
    Thread.sleep(5000);

    // retrieve current schema
    SchemaRequest schemaRequest = new SchemaRequest();
    SchemaResponse schemaResponse = schemaRequest.process(cloudClient);
    Map<String, Object> field = getSchemaField(TEST_FIELD, schemaResponse);
    assertNotNull("missing " + TEST_FIELD + " field", field);
    assertEquals("wrong flags: " + field, Boolean.FALSE, field.get("docValues"));

    // update schema
    field.put("docValues", true);
    SchemaRequest.ReplaceField replaceRequest = new SchemaRequest.ReplaceField(field);
    SchemaResponse.UpdateResponse replaceResponse = replaceRequest.process(cloudClient);

    log.info("-- completed schema update");

    // bounce the collection
    Map<String, Long> urlToTimeBefore = new HashMap<>();
    collectStartTimes(collectionName, cloudClient, urlToTimeBefore);
    CollectionAdminRequest<CollectionAdminRequest.Reload> reload = new CollectionAdminRequest.Reload()
        .setCollectionName(collectionName);
    rsp = reload.process(cloudClient);

    boolean reloaded = waitForReloads(collectionName, cloudClient, urlToTimeBefore);
    assertTrue("could not reload collection in time", reloaded);

    log.info("-- completed collection reload");

    // verify that schema doesn't match the actual fields anymore
    rsp = status.process(cloudClient);
    log.info("--rsp: {}", rsp);
    nonCompliant = (List<String>)rsp.getResponse().findRecursive(collectionName, "schemaNonCompliant");
    assertNotNull("nonCompliant missing: " + rsp, nonCompliant);
    assertEquals("nonCompliant: " + nonCompliant, 1, nonCompliant.size());
    assertEquals("nonCompliant: " + nonCompliant, TEST_FIELD, nonCompliant.get(0));


    // update plugin props to allow merging
    pluginProps.put(AddDocValuesMergePolicyFactory.NO_MERGE_PROP, false);
    propValue = Utils.toJSONString(pluginProps);
    clusterProp = new CollectionAdminRequest.ClusterProp()
        .setPropertyName(PluggableMergePolicyFactory.MERGE_POLICY_PROP + collectionName)
        .setPropertyValue(propValue);
    clusterProp.process(cloudClient);

    log.info("-- completed set cluster props 2");

    urlToTimeBefore = new HashMap<>();
    collectStartTimes(collectionName, cloudClient, urlToTimeBefore);
    rsp = reload.process(cloudClient);
    reloaded = waitForReloads(collectionName, cloudClient, urlToTimeBefore);
    assertTrue("could not reload collection in time", reloaded);

    // verify that schema doesn't match the actual fields anymore
    rsp = status.process(cloudClient);
    nonCompliant = (List<String>)rsp.getResponse().findRecursive(collectionName, "schemaNonCompliant");
    assertNotNull("nonCompliant missing: " + rsp, nonCompliant);
    assertEquals("nonCompliant: " + nonCompliant, 1, nonCompliant.size());
    assertEquals("nonCompliant: " + nonCompliant, TEST_FIELD, nonCompliant.get(0));


    log.info("-- start optimize");
    // request optimize to make sure all segments are rewritten
    cloudClient.optimize(collectionName, true, true, 1);
    cloudClient.commit();

    log.info("-- completed optimize");

    rsp = status.process(cloudClient);
    nonCompliant = (List<String>)rsp.getResponse().findRecursive(collectionName, "schemaNonCompliant");
    assertNotNull("nonCompliant missing: " + rsp, nonCompliant);
    assertEquals("nonCompliant: " + nonCompliant, 1, nonCompliant.size());
    assertEquals("nonCompliant: " + nonCompliant, "(NONE)", nonCompliant.get(0));
    runIndexer.set(false);

    // verify that all docs have docValues
    for (JettySolrRunner jetty : jettys) {
      CoreContainer cores = ((SolrDispatchFilter)jetty.getDispatchFilter().getFilter()).getCores();
      for (SolrCore core : cores.getCores()) {
        RefCounted<SolrIndexSearcher> searcherRef = core.getSearcher();
        SolrIndexSearcher searcher = searcherRef.get();
        try {
          LeafReader reader = searcher.getLeafReader();
          int maxDoc = reader.maxDoc();
          SortedDocValues dvs = reader.getSortedDocValues(TEST_FIELD);
          for (int i = 0; i < maxDoc; i++) {
            Document d = reader.document(i);
            BytesRef bytes = dvs.get(i);
            assertNotNull(bytes);
            String dvString = bytes.utf8ToString();
            assertEquals(d.get("id"), dvString);
          }
          DirectoryReader directoryReader = searcher.getIndexReader();
          for (LeafReaderContext leafCtx : directoryReader.leaves()) {
            LeafReader leaf = leafCtx.reader();
            while (leaf instanceof FilterLeafReader) {
              leaf = ((FilterLeafReader)leaf).getDelegate();
            }
            assertTrue(leaf instanceof SegmentReader);
            SegmentReader segmentReader = (SegmentReader)leaf;
            String marker = segmentReader.getSegmentInfo().info.getDiagnostics().get(AddDocValuesMergePolicyFactory.DIAGNOSTICS_MARKER_PROP);
            // new flush segments that are fully compliant won't have
            // the marker because they were not wrapped
            if (marker != null) {
              assertEquals(AddDocValuesMergePolicyFactory.DEFAULT_MARKER, marker);
            }
          }
        } finally {
          searcherRef.decref();
        }
      }
    }
  }

  private Map<String, Object> getSchemaField(String name, SchemaResponse schemaResponse) {
    List<Map<String, Object>> fields = schemaResponse.getSchemaRepresentation().getFields();
    for (Map<String, Object> field : fields) {
      if (name.equals(field.get("name"))) {
        return field;
      }
    }
    return null;
  }
}
