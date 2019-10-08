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
package org.apache.solr.cloud;

import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.search.similarities.CustomSimilarityFactory;
import org.apache.solr.search.stats.StatsCache;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
@Ignore("Abstract classes should not be executed as tests")
public abstract class TestBaseStatsCacheCloud extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected int numNodes = 2;
  protected String configset = "cloud-dynamic";

  protected String collectionName = "collection_" + getClass().getSimpleName();

  protected Function<Integer, SolrInputDocument> generator = i -> {
    SolrInputDocument doc = new SolrInputDocument("id", "id-" + i);
    if (i % 3 == 0) {
      doc.addField("foo_t", "bar baz");
    } else if (i % 3 == 1) {
      doc.addField("foo_t", "bar");
    } else {
      // skip the field
    }
    return doc;
  };

  protected CloudSolrClient solrClient;

  protected SolrClient control;

  protected int NUM_DOCS = 100;

  // implementation name
  protected abstract String getImplementationName();

  // does this implementation produce the same distrib scores as local ones?
  protected abstract boolean assertSameScores();

  @Before
  public void setupCluster() throws Exception {
    // create control core & client
    System.setProperty("solr.statsCache", getImplementationName());
    System.setProperty("solr.similarity", CustomSimilarityFactory.class.getName());
    initCore("solrconfig-minimal.xml", "schema-tiny.xml");
    control = new EmbeddedSolrServer(h.getCore());
    // create cluster
    configureCluster(numNodes) // 2 + random().nextInt(3)
        .addConfig("conf", configset(configset))
        .configure();
    solrClient = cluster.getSolrClient();
    createTestCollection();
  }

  protected void createTestCollection() throws Exception {
    CollectionAdminRequest.createCollection(collectionName, "conf", 2, numNodes)
        .setMaxShardsPerNode(2)
        .process(solrClient);
    indexDocs(solrClient, collectionName, NUM_DOCS, 0, generator);
    indexDocs(control, "collection1", NUM_DOCS, 0, generator);
  }

  @After
  public void tearDownCluster() {
    System.clearProperty("solr.statsCache");
    System.clearProperty("solr.similarity");
  }

  @Test
  public void testBasicStats() throws Exception {
    QueryResponse cloudRsp = solrClient.query(collectionName,
        params("q", "foo_t:\"bar baz\"", "fl", "*,score", "rows", "" + NUM_DOCS, "debug", "true"));
    QueryResponse controlRsp = control.query("collection1",
        params("q", "foo_t:\"bar baz\"", "fl", "*,score", "rows", "" + NUM_DOCS, "debug", "true"));

    assertResponses(controlRsp, cloudRsp, assertSameScores());

    // test after updates
    indexDocs(solrClient, collectionName, NUM_DOCS, NUM_DOCS, generator);
    indexDocs(control, "collection1", NUM_DOCS, NUM_DOCS, generator);

    cloudRsp = solrClient.query(collectionName,
        params("q", "foo_t:\"bar baz\"", "fl", "*,score", "rows", "" + (NUM_DOCS * 2)));
    controlRsp = control.query("collection1",
        params("q", "foo_t:\"bar baz\"", "fl", "*,score", "rows", "" + (NUM_DOCS * 2)));
    assertResponses(controlRsp, cloudRsp, assertSameScores());

    // check cache metrics
    StatsCache.StatsCacheMetrics statsCacheMetrics = new StatsCache.StatsCacheMetrics();
    for (JettySolrRunner jettySolrRunner : cluster.getJettySolrRunners()) {
      try (SolrClient client = getHttpSolrClient(jettySolrRunner.getBaseUrl().toString())) {
        NamedList<Object> metricsRsp = client.request(
            new GenericSolrRequest(SolrRequest.METHOD.GET, "/admin/metrics", params("group", "solr.core", "prefix", "CACHE.searcher.statsCache")));
        assertNotNull(metricsRsp);
        NamedList<Object> metricsPerReplica = (NamedList<Object>)metricsRsp.get("metrics");
        assertNotNull("no metrics perReplica", metricsPerReplica);
        //log.info("======= Node: " + jettySolrRunner.getBaseUrl());
        //log.info("======= Metrics:\n" + Utils.toJSONString(metricsPerReplica));
        metricsPerReplica.forEach((replica, metrics) -> {
          Map<String, Object> values = (Map<String, Object>)((NamedList<Object>)metrics).get("CACHE.searcher.statsCache");
          values.forEach((name, value) -> {
            long val = value instanceof Number ? ((Number) value).longValue() : 0;
            switch (name) {
              case "lookups" :
                statsCacheMetrics.lookups.add(val);
                break;
              case "returnLocalStats" :
                statsCacheMetrics.returnLocalStats.add(val);
                break;
              case "mergeToGlobalStats" :
                statsCacheMetrics.mergeToGlobalStats.add(val);
                break;
              case "missingGlobalFieldStats" :
                statsCacheMetrics.missingGlobalFieldStats.add(val);
                break;
              case "missingGlobalTermStats" :
                statsCacheMetrics.missingGlobalTermStats.add(val);
                break;
              case "receiveGlobalStats" :
                statsCacheMetrics.receiveGlobalStats.add(val);
                break;
              case "retrieveStats" :
                statsCacheMetrics.retrieveStats.add(val);
                break;
              case "sendGlobalStats" :
                statsCacheMetrics.sendGlobalStats.add(val);
                break;
              case "useCachedGlobalStats" :
                statsCacheMetrics.useCachedGlobalStats.add(val);
                break;
              case "statsCacheImpl" :
                assertTrue("incorreect cache impl, expected" + getImplementationName() + " but was " + value,
                    getImplementationName().endsWith((String)value));
                break;
              default:
                fail("Unexpected cache metrics: key=" + name + ", value=" + value);
            }
          });
        });
      }
    }
    checkStatsCacheMetrics(statsCacheMetrics);
  }

  protected void checkStatsCacheMetrics(StatsCache.StatsCacheMetrics statsCacheMetrics) {
    assertEquals(statsCacheMetrics.toString(), 0, statsCacheMetrics.missingGlobalFieldStats.intValue());
    assertEquals(statsCacheMetrics.toString(), 0, statsCacheMetrics.missingGlobalTermStats.intValue());
  }

  protected void assertResponses(QueryResponse controlRsp, QueryResponse cloudRsp, boolean sameScores) throws Exception {
    Map<String, SolrDocument> cloudDocs = new HashMap<>();
    Map<String, SolrDocument> controlDocs = new HashMap<>();
    cloudRsp.getResults().forEach(doc -> cloudDocs.put((String) doc.getFieldValue("id"), doc));
    controlRsp.getResults().forEach(doc -> controlDocs.put((String) doc.getFieldValue("id"), doc));
    assertEquals("number of docs", controlDocs.size(), cloudDocs.size());
    for (Map.Entry<String, SolrDocument> entry : controlDocs.entrySet()) {
      SolrDocument controlDoc = entry.getValue();
      SolrDocument cloudDoc = cloudDocs.get(entry.getKey());
      assertNotNull("missing cloud doc " + controlDoc, cloudDoc);
      Float controlScore = (Float) controlDoc.getFieldValue("score");
      Float cloudScore = (Float) cloudDoc.getFieldValue("score");
      if (sameScores) {
        assertEquals("cloud score differs from control", controlScore, cloudScore, controlScore * 0.01f);
      } else {
        assertFalse("cloud score the same as control", controlScore == cloudScore);
      }
    }
  }

  protected void indexDocs(SolrClient client, String collectionName, int num, int start, Function<Integer, SolrInputDocument> generator) throws Exception {

    UpdateRequest ureq = new UpdateRequest();
    for (int i = 0; i < num; i++) {
      SolrInputDocument doc = generator.apply(i + start);
      ureq.add(doc);
    }
    ureq.process(client, collectionName);
    client.commit(collectionName);
  }
}
