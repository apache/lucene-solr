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
package org.apache.solr.handler.admin;

import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.document.DocumentStoredFieldVisitor;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.core.SolrCore;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.RefCounted;
import org.apache.solr.util.TimeOut;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class IndexSizeEstimatorTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static CloudSolrClient solrClient;
  private static String collection = IndexSizeEstimator.class.getSimpleName() + "_collection";
  private static int NUM_DOCS = 2000;
  private static Set<String> fields;

  @BeforeClass
  public static void setupCluster() throws Exception {
    // create predictable field names
    System.setProperty("solr.tests.numeric.dv", "true");
    System.setProperty("solr.tests.numeric.points", "true");
    System.setProperty("solr.tests.numeric.points.dv", "true");
    configureCluster(2)
        .addConfig("conf", configset("cloud-dynamic"))
        .configure();
    solrClient = cluster.getSolrClient();
    CollectionAdminRequest.createCollection(collection, "conf", 2, 2)
        .setMaxShardsPerNode(2).process(solrClient);
    cluster.waitForActiveCollection(collection, 2, 4);
    SolrInputDocument lastDoc = addDocs(collection, NUM_DOCS);
    HashSet<String> docFields = new HashSet<>(lastDoc.keySet());
    docFields.add("_version_");
    docFields.add("_root_");
    docFields.add("point_0___double");
    docFields.add("point_1___double");
    fields = docFields;
  }

  @AfterClass
  public static void releaseClient() throws Exception {
    solrClient = null;
  }

  @Test
  public void testEstimator() throws Exception {
    JettySolrRunner jetty = cluster.getRandomJetty(random());
    String randomCoreName = jetty.getCoreContainer().getAllCoreNames().iterator().next();
    SolrCore core = jetty.getCoreContainer().getCore(randomCoreName);
    RefCounted<SolrIndexSearcher> searcherRef = core.getSearcher();
    try {
      SolrIndexSearcher searcher = searcherRef.get();
      // limit the max length
      IndexSizeEstimator estimator = new IndexSizeEstimator(searcher.getRawReader(), 20, 50, true, true);
      IndexSizeEstimator.Estimate estimate = estimator.estimate();
      Map<String, Long> fieldsBySize = estimate.getFieldsBySize();
      assertFalse("empty fieldsBySize", fieldsBySize.isEmpty());
      assertEquals(fieldsBySize.toString(), fields.size(), fieldsBySize.size());
      fieldsBySize.forEach((k, v) -> assertTrue("unexpected size of " + k + ": " + v, v > 0));
      Map<String, Long> typesBySize = estimate.getTypesBySize();
      assertFalse("empty typesBySize", typesBySize.isEmpty());
      assertTrue("expected at least 8 types: " + typesBySize.toString(), typesBySize.size() >= 8);
      typesBySize.forEach((k, v) -> assertTrue("unexpected size of " + k + ": " + v, v > 0));
      Map<String, Object> summary = estimate.getSummary();
      assertNotNull("summary", summary);
      assertFalse("empty summary", summary.isEmpty());
      assertEquals(summary.keySet().toString(), fields.size(), summary.keySet().size());
      Map<String, Object> details = estimate.getDetails();
      assertNotNull("details", details);
      assertFalse("empty details", details.isEmpty());
      // by type
      assertEquals(details.keySet().toString(), 6, details.keySet().size());

      // check sampling
      estimator.setSamplingThreshold(searcher.getRawReader().maxDoc() / 2);
      IndexSizeEstimator.Estimate sampledEstimate = estimator.estimate();
      Map<String, Long> sampledFieldsBySize = sampledEstimate.getFieldsBySize();
      assertFalse("empty fieldsBySize", sampledFieldsBySize.isEmpty());
      // verify that the sampled values are within 50% of the original values
      fieldsBySize.forEach((field, size) -> {
        Long sampledSize = sampledFieldsBySize.get(field);
        assertNotNull("sampled size for " + field + " is missing in " + sampledFieldsBySize, sampledSize);
        double delta = (double) size * 0.5;
        assertEquals("sampled size of " + field + " is wildly off", (double)size, (double)sampledSize, delta);
      });
      // verify the reader is still usable - SOLR-13694
      IndexReader reader = searcher.getRawReader();
      for (LeafReaderContext context : reader.leaves()) {
        LeafReader leafReader = context.reader();
        assertTrue("unexpected LeafReader class: " + leafReader.getClass().getName(), leafReader instanceof CodecReader);
        Bits liveDocs = leafReader.getLiveDocs();
        CodecReader codecReader = (CodecReader) leafReader;
        StoredFieldsReader storedFieldsReader = codecReader.getFieldsReader();
        StoredFieldVisitor visitor = new DocumentStoredFieldVisitor();
        assertNotNull(storedFieldsReader);
        for (int docId = 0; docId < leafReader.maxDoc(); docId++) {
          if (liveDocs != null && !liveDocs.get(docId)) {
            continue;
          }
          storedFieldsReader.visitDocument(docId, visitor);
        }
      }
    } finally {
      searcherRef.decref();
      core.close();
    }
  }

  @Test
  public void testIntegration() throws Exception {
    CollectionAdminResponse rsp = CollectionAdminRequest.collectionStatus(collection)
        .setWithRawSizeInfo(true)
        .setWithRawSizeSummary(true)
        .setWithRawSizeDetails(true)
        .process(solrClient);
    CollectionAdminResponse sampledRsp = CollectionAdminRequest.collectionStatus(collection)
        .setWithRawSizeInfo(true)
        .setWithRawSizeSummary(true)
        .setWithRawSizeDetails(true)
        .setRawSizeSamplingPercent(5)
        .process(solrClient);
    assertEquals(0, rsp.getStatus());
    assertEquals(0, sampledRsp.getStatus());
    for (int i : Arrays.asList(1, 2)) {
      NamedList<Object> segInfos = (NamedList<Object>) rsp.getResponse().findRecursive(collection, "shards", "shard" + i, "leader", "segInfos");
      NamedList<Object> rawSize = (NamedList<Object>)segInfos.get("rawSize");
      assertNotNull("rawSize missing", rawSize);
      Map<String, Object> rawSizeMap = rawSize.asMap(10);
      Map<String, Object> fieldsBySize = (Map<String, Object>)rawSizeMap.get(IndexSizeEstimator.FIELDS_BY_SIZE);
      assertNotNull("fieldsBySize missing", fieldsBySize);
      assertEquals(fieldsBySize.toString(), fields.size(), fieldsBySize.size());
      fields.forEach(field -> assertNotNull("missing field " + field, fieldsBySize.get(field)));
      Map<String, Object> typesBySize = (Map<String, Object>)rawSizeMap.get(IndexSizeEstimator.TYPES_BY_SIZE);
      assertNotNull("typesBySize missing", typesBySize);
      assertTrue("expected at least 8 types: " + typesBySize.toString(), typesBySize.size() >= 8);
      Map<String, Object> summary = (Map<String, Object>)rawSizeMap.get(IndexSizeEstimator.SUMMARY);
      assertNotNull("summary missing", summary);
      assertEquals(summary.toString(), fields.size(), summary.size());
      fields.forEach(field -> assertNotNull("missing field " + field, summary.get(field)));
      Map<String, Object> details = (Map<String, Object>)rawSizeMap.get(IndexSizeEstimator.DETAILS);
      assertNotNull("details missing", summary);
      assertEquals(details.keySet().toString(), 6, details.size());

      // compare with sampled
      NamedList<Object> sampledRawSize = (NamedList<Object>) rsp.getResponse().findRecursive(collection, "shards", "shard" + i, "leader", "segInfos", "rawSize");
      assertNotNull("sampled rawSize missing", sampledRawSize);
      Map<String, Object> sampledRawSizeMap = rawSize.asMap(10);
      Map<String, Object> sampledFieldsBySize = (Map<String, Object>)sampledRawSizeMap.get(IndexSizeEstimator.FIELDS_BY_SIZE);
      assertNotNull("sampled fieldsBySize missing", sampledFieldsBySize);
      fieldsBySize.forEach((k, v) -> {
        double size = fromHumanReadableUnits((String)v);
        double sampledSize = fromHumanReadableUnits((String)sampledFieldsBySize.get(k));
        assertNotNull("sampled size missing for field " + k + " in " + sampledFieldsBySize, sampledSize);
        double delta = size * 0.5;
        assertEquals("sampled size of " + k + " is wildly off", size, sampledSize, delta);
      });
    }

  }

  private static double fromHumanReadableUnits(String value) {
    String[] parts = value.split(" ");
    assertEquals("invalid value", 2, parts.length);
    double result = Double.parseDouble(parts[0]);
    if (parts[1].equals("GB")) {
      result = result * RamUsageEstimator.ONE_GB;
    } else if (parts[1].equals("MB")) {
      result = result * RamUsageEstimator.ONE_MB;
    } else if (parts[1].equals("KB")) {
      result = result * RamUsageEstimator.ONE_KB;
    } else if (parts[1].equals("bytes")) {
      // do nothing
    } else {
      fail("invalid unit in " + value);
    }
    return result;
  }

  private static SolrInputDocument addDocs(String collection, int n) throws Exception {
    UpdateRequest ureq = new UpdateRequest();
    SolrInputDocument doc = null;
    for (int i = 0; i < n; i++) {
      doc = new SolrInputDocument();
      doc.addField("id", "id-" + i);
      doc.addField("long_l", i);
      doc.addField("long_tl", i);
      doc.addField("multival_long_ll", i);
      doc.addField("multival_long_ll", i + 1);
      // indexed, not stored
      doc.addField("string_sI", TestUtil.randomAnalysisString(random(), 100, true));
      // stored, not indexed
      doc.addField("string_sS", TestUtil.randomAnalysisString(random(), 100, true));
      // multival, stored, indexed, tv, pos, offsets
      doc.addField("tv_mv_string", TestUtil.randomAnalysisString(random(), 100, true));
      doc.addField("tv_mv_string", TestUtil.randomAnalysisString(random(), 100, true));
      //binary
      doc.addField("payload", TestUtil.randomBinaryTerm(random()).bytes);
      // points
      doc.addField("point", random().nextInt(100) + "," + random().nextInt(100));
      ureq.add(doc);
    }
    solrClient.request(ureq, collection);
    solrClient.commit(collection);
    // verify the number of docs
    TimeOut timeOut = new TimeOut(30, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    while (!timeOut.hasTimedOut()) {
      QueryResponse rsp = solrClient.query(collection, params("q", "*:*", "rows", "0"));
      if (rsp.getResults().getNumFound() == n) {
        break;
      }
      timeOut.sleep(500);
    }
    assertFalse("timed out waiting for documents to be added", timeOut.hasTimedOut());
    return doc;
  }

}
