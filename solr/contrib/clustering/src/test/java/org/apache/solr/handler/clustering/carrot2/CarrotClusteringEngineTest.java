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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

import com.google.common.collect.ImmutableList;

/**
 *
 */
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
    checkClusters(checkEngine(getClusteringEngine("mock"), AbstractClusteringTestCase.numberOfDocs),
            1, 1, 0);
  }

  @Test
  public void testWithSubclusters() throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(CarrotParams.OUTPUT_SUB_CLUSTERS, true);
    checkClusters(checkEngine(getClusteringEngine("mock"), AbstractClusteringTestCase.numberOfDocs), 1, 1, 2);
  }

  @Test
  public void testNumDescriptions() throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(AttributeUtils.getKey(MockClusteringAlgorithm.class, "labels"), 5);
    params.set(CarrotParams.NUM_DESCRIPTIONS, 3);
    checkClusters(checkEngine(getClusteringEngine("mock"), AbstractClusteringTestCase.numberOfDocs,
            params), 1, 3, 0);
  }

  @Test
  public void testClusterScores() throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(AttributeUtils.getKey(MockClusteringAlgorithm.class, "depth"), 1);
    List<NamedList<Object>> clusters = checkEngine(getClusteringEngine("mock"),
        AbstractClusteringTestCase.numberOfDocs, params);
    int i = 1;
    for (NamedList<Object> cluster : clusters) {
      final Double score = getScore(cluster);
      assertNotNull(score);
      assertEquals(0.25 * i++, score, 0);
    }
  }

  @Test
  public void testOtherTopics() throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(AttributeUtils.getKey(MockClusteringAlgorithm.class, "depth"), 1);
    params.set(AttributeUtils.getKey(MockClusteringAlgorithm.class, "otherTopicsModulo"), 2);
    List<NamedList<Object>> clusters = checkEngine(getClusteringEngine("mock"),
        AbstractClusteringTestCase.numberOfDocs, params);
    int i = 1;
    for (NamedList<Object> cluster : clusters) {
      assertEquals(i++ % 2 == 0 ? true : null, isOtherTopics(cluster));
    }
  }

  @Test
  public void testCarrotAttributePassing() throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(AttributeUtils.getKey(MockClusteringAlgorithm.class, "depth"), 1);
    params.set(AttributeUtils.getKey(MockClusteringAlgorithm.class, "labels"), 3);
    checkClusters(checkEngine(getClusteringEngine("mock"), AbstractClusteringTestCase.numberOfDocs,
            params), 1, 3, 0);
  }

	@Test
	public void testLexicalResourcesFromSolrConfigDefaultDir() throws Exception {
		checkLexicalResourcesFromSolrConfig("lexical-resource-check",
				"online,customsolrstopword,customsolrstoplabel");
	}

	@Test
	public void testLexicalResourcesFromSolrConfigCustomDir() throws Exception {
		checkLexicalResourcesFromSolrConfig("lexical-resource-check-custom-resource-dir",
				"online,customsolrstopwordcustomdir,customsolrstoplabelcustomdir");
	}

	private void checkLexicalResourcesFromSolrConfig(String engineName, String wordsToCheck)
			throws IOException {
		ModifiableSolrParams params = new ModifiableSolrParams();
		params.set("merge-resources", false);
		params.set(AttributeUtils.getKey(
				LexicalResourcesCheckClusteringAlgorithm.class, "wordsToCheck"),
				wordsToCheck);

		// "customsolrstopword" is in stopwords.en, "customsolrstoplabel" is in
		// stoplabels.en, so we're expecting only one cluster with label "online".
		final List<NamedList<Object>> clusters = checkEngine(
				getClusteringEngine(engineName), 1, params);
		assertEquals(getLabels(clusters.get(0)), ImmutableList.of("online"));
	}

	@Test
	public void solrStopWordsUsedInCarrot2Clustering() throws Exception {
		ModifiableSolrParams params = new ModifiableSolrParams();
		params.set("merge-resources", false);
		params.set(AttributeUtils.getKey(
				LexicalResourcesCheckClusteringAlgorithm.class, "wordsToCheck"),
		"online,solrownstopword");

		// "solrownstopword" is in stopwords.txt, so we're expecting
		// only one cluster with label "online".
		final List<NamedList<Object>> clusters = checkEngine(
				getClusteringEngine("lexical-resource-check"), 1, params);
		assertEquals(getLabels(clusters.get(0)), ImmutableList.of("online"));
	}

	@Test
	public void solrStopWordsNotDefinedOnAFieldForClustering() throws Exception {
		ModifiableSolrParams params = new ModifiableSolrParams();
		// Force string fields to be used for clustering. Does not make sense
		// in a real word, but does the job in the test.
		params.set(CarrotParams.TITLE_FIELD_NAME, "url");
		params.set(CarrotParams.SNIPPET_FIELD_NAME, "url");
		params.set("merge-resources", false);
		params.set(AttributeUtils.getKey(
				LexicalResourcesCheckClusteringAlgorithm.class, "wordsToCheck"),
		"online,solrownstopword");

		final List<NamedList<Object>> clusters = checkEngine(
				getClusteringEngine("lexical-resource-check"), 2, params);
		assertEquals(ImmutableList.of("online"), getLabels(clusters.get(0)));
		assertEquals(ImmutableList.of("solrownstopword"),
				getLabels(clusters.get(1)));
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

  private List<NamedList<Object>> checkEngine(CarrotClusteringEngine engine,
                            int expectedNumClusters) throws IOException {
    return checkEngine(engine, numberOfDocs, expectedNumClusters, new MatchAllDocsQuery(), new ModifiableSolrParams());
  }

  private List<NamedList<Object>> checkEngine(CarrotClusteringEngine engine,
                            int expectedNumClusters, SolrParams clusteringParams) throws IOException {
    return checkEngine(engine, numberOfDocs, expectedNumClusters, new MatchAllDocsQuery(), clusteringParams);
  }


  private List<NamedList<Object>> checkEngine(CarrotClusteringEngine engine, int expectedNumDocs,
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

      @SuppressWarnings("unchecked")
			List<NamedList<Object>> results = (List<NamedList<Object>>) engine.cluster(query, solrDocList, docIds, req);
      req.close();
      assertEquals("number of clusters: " + results, expectedNumClusters, results.size());
      checkClusters(results, false);
      return results;
    } finally {
      ref.decref();
    }
  }

  private void checkClusters(List<NamedList<Object>> results, int expectedDocCount,
                             int expectedLabelCount, int expectedSubclusterCount) {
    for (int i = 0; i < results.size(); i++) {
      NamedList<Object> cluster = results.get(i);
      checkCluster(cluster, expectedDocCount, expectedLabelCount,
              expectedSubclusterCount);
    }
  }

  private void checkClusters(List<NamedList<Object>> results, boolean hasSubclusters) {
    for (int i = 0; i < results.size(); i++) {
      checkCluster(results.get(i), hasSubclusters);
    }
  }

  private void checkCluster(NamedList<Object> cluster, boolean hasSubclusters) {
    List<Object> docs = getDocs(cluster);
    assertNotNull("docs is null and it shouldn't be", docs);
    for (int j = 0; j < docs.size(); j++) {
      String id = (String) docs.get(j);
      assertNotNull("id is null and it shouldn't be", id);
    }

    List<String> labels = getLabels(cluster);
    assertNotNull("labels is null but it shouldn't be", labels);

    if (hasSubclusters) {
      List<NamedList<Object>> subclusters = getSubclusters(cluster);
      assertNotNull("subclusters is null but it shouldn't be", subclusters);
    }
  }

  private void checkCluster(NamedList<Object> cluster, int expectedDocCount,
                            int expectedLabelCount, int expectedSubclusterCount) {
    checkCluster(cluster, expectedSubclusterCount > 0);
    assertEquals("number of docs in cluster", expectedDocCount,
            getDocs(cluster).size());
    assertEquals("number of labels in cluster", expectedLabelCount,
            getLabels(cluster).size());

    if (expectedSubclusterCount > 0) {
      List<NamedList<Object>> subclusters = getSubclusters(cluster);
      assertEquals("numClusters", expectedSubclusterCount, subclusters.size());
      assertEquals("number of subclusters in cluster",
              expectedSubclusterCount, subclusters.size());
    }
  }

	@SuppressWarnings("unchecked")
	private List<NamedList<Object>> getSubclusters(NamedList<Object> cluster) {
		return (List<NamedList<Object>>) cluster.get("clusters");
	}

	@SuppressWarnings("unchecked")
	private List<String> getLabels(NamedList<Object> cluster) {
		return (List<String>) cluster.get("labels");
	}

	private Double getScore(NamedList<Object> cluster) {
	  return (Double) cluster.get("score");
	}

	private Boolean isOtherTopics(NamedList<Object> cluster) {
	  return (Boolean)cluster.get("other-topics");
	}

	@SuppressWarnings("unchecked")
	private List<Object> getDocs(NamedList<Object> cluster) {
		return (List<Object>) cluster.get("docs");
	}
}
