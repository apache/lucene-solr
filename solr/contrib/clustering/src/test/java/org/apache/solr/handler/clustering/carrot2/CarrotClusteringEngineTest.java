package org.apache.solr.handler.clustering.carrot2;

/**
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

import org.apache.lucene.index.Term;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TermQuery;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.clustering.AbstractClusteringTestCase;
import org.apache.solr.handler.clustering.ClusteringComponent;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.search.DocList;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.RefCounted;
import org.apache.solr.util.SolrPluginUtils;
import org.carrot2.util.attribute.AttributeUtils;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
@SuppressWarnings("unchecked")
public class CarrotClusteringEngineTest extends AbstractClusteringTestCase {
  @Test
  public void testCarrotLingo() throws Exception {
  	// Note: the expected number of clusters may change after upgrading Carrot2
  	// due to e.g. internal improvements or tuning of Carrot2 clustering.
    final int expectedNumClusters = 10;
		checkEngine(getClusteringEngine("default"), expectedNumClusters);
  }

  @Test
  public void testProduceSummary() throws Exception {
    ModifiableSolrParams solrParams = new ModifiableSolrParams();
    solrParams.add(CarrotParams.SNIPPET_FIELD_NAME, "snippet");
    solrParams.add(CarrotParams.SUMMARY_FRAGSIZE, "200");//how do we validate this?
    
  	// Note: the expected number of clusters may change after upgrading Carrot2
  	// due to e.g. internal improvements or tuning of Carrot2 clustering.
    final int expectedNumClusters = 15;
    checkEngine(getClusteringEngine("default"), numberOfDocs -2 /*two don't have mining in the snippet*/, expectedNumClusters, new TermQuery(new Term("snippet", "mine")), solrParams);
  }

  @Test
  public void testCarrotStc() throws Exception {
    checkEngine(getClusteringEngine("stc"), 1);
  }

  @Test
  public void testWithoutSubclusters() throws Exception {
    checkClusters(checkEngine(getClusteringEngine("mock"), this.numberOfDocs),
            1, 1, 0);
  }

  @Test
  public void testWithSubclusters() throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(CarrotParams.OUTPUT_SUB_CLUSTERS, true);
    checkClusters(checkEngine(getClusteringEngine("mock"), this.numberOfDocs), 1, 1, 2);
  }

  @Test
  public void testNumDescriptions() throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(AttributeUtils.getKey(MockClusteringAlgorithm.class, "labels"), 5);
    params.set(CarrotParams.NUM_DESCRIPTIONS, 3);
    checkClusters(checkEngine(getClusteringEngine("mock"), this.numberOfDocs,
            params), 1, 3, 0);
  }

  @Test
  public void testCarrotAttributePassing() throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(AttributeUtils.getKey(MockClusteringAlgorithm.class, "depth"), 1);
    params.set(AttributeUtils.getKey(MockClusteringAlgorithm.class, "labels"), 3);
    checkClusters(checkEngine(getClusteringEngine("mock"), this.numberOfDocs,
            params), 1, 3, 0);
  }

  private CarrotClusteringEngine getClusteringEngine(String engineName) {
    ClusteringComponent comp = (ClusteringComponent) h.getCore()
            .getSearchComponent("clustering");
    assertNotNull("clustering component should not be null", comp);
    CarrotClusteringEngine engine = (CarrotClusteringEngine) comp
            .getSearchClusteringEngines().get(engineName);
    assertNotNull("clustering engine for name: " + engineName
            + " should not be null", engine);
    return engine;
  }

  private List checkEngine(CarrotClusteringEngine engine,
                            int expectedNumClusters) throws IOException {
    return checkEngine(engine, numberOfDocs, expectedNumClusters, new MatchAllDocsQuery(), new ModifiableSolrParams());
  }

  private List checkEngine(CarrotClusteringEngine engine,
                            int expectedNumClusters, SolrParams clusteringParams) throws IOException {
    return checkEngine(engine, numberOfDocs, expectedNumClusters, new MatchAllDocsQuery(), clusteringParams);
  }


  private List checkEngine(CarrotClusteringEngine engine, int expectedNumDocs,
                           int expectedNumClusters, Query query, SolrParams clusteringParams) throws IOException {
    // Get all documents to cluster
    RefCounted<SolrIndexSearcher> ref = h.getCore().getSearcher();

    DocList docList;
    try {
      SolrIndexSearcher searcher = ref.get();
      docList = searcher.getDocList(query, (Query) null, new Sort(), 0,
              numberOfDocs);
      assertEquals("docList size", expectedNumDocs, docList.matches());

      ModifiableSolrParams solrParams = new ModifiableSolrParams();
      solrParams.add(CarrotParams.PRODUCE_SUMMARY, "true");
      solrParams.add(clusteringParams);

      // Perform clustering
      LocalSolrQueryRequest req = new LocalSolrQueryRequest(h.getCore(), solrParams);
      Map<SolrDocument,Integer> docIds = new HashMap<SolrDocument, Integer>(docList.size());
      SolrDocumentList solrDocList = SolrPluginUtils.docListToSolrDocumentList( docList, searcher, engine.getFieldsToLoad(req), docIds );
      List results = (List)engine.cluster(query, solrDocList, docIds, req);
      req.close();
      assertEquals("number of clusters: " + results, expectedNumClusters, results.size());
      checkClusters(results, false);
      return results;
    } finally {
      ref.decref();
    }
  }

  private void checkClusters(List results, int expectedDocCount,
                             int expectedLabelCount, int expectedSubclusterCount) {
    for (int i = 0; i < results.size(); i++) {
      NamedList cluster = (NamedList) results.get(i);
      checkCluster(cluster, expectedDocCount, expectedLabelCount,
              expectedSubclusterCount);
    }
  }

  private void checkClusters(List results, boolean hasSubclusters) {
    for (int i = 0; i < results.size(); i++) {
      checkCluster((NamedList) results.get(i), hasSubclusters);
    }
  }

  private void checkCluster(NamedList cluster, boolean hasSubclusters) {
    List docs = (List) cluster.get("docs");
    assertNotNull("docs is null and it shouldn't be", docs);
    for (int j = 0; j < docs.size(); j++) {
      String id = (String) docs.get(j);
      assertNotNull("id is null and it shouldn't be", id);
    }

    List labels = (List) cluster.get("labels");
    assertNotNull("labels is null but it shouldn't be", labels);

    if (hasSubclusters) {
      List subclusters = (List) cluster.get("clusters");
      assertNotNull("subclusters is null but it shouldn't be", subclusters);
    }
  }

  private void checkCluster(NamedList cluster, int expectedDocCount,
                            int expectedLabelCount, int expectedSubclusterCount) {
    checkCluster(cluster, expectedSubclusterCount > 0);
    assertEquals("number of docs in cluster", expectedDocCount,
            ((List) cluster.get("docs")).size());
    assertEquals("number of labels in cluster", expectedLabelCount,
            ((List) cluster.get("labels")).size());

    if (expectedSubclusterCount > 0) {
      List subclusters = (List) cluster.get("clusters");
      assertEquals("numClusters", expectedSubclusterCount, subclusters.size());
      assertEquals("number of subclusters in cluster",
              expectedSubclusterCount, subclusters.size());
    }
  }
}
