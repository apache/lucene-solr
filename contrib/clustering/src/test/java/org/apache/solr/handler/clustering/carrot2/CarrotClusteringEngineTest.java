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

import org.apache.lucene.search.*;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.clustering.AbstractClusteringTest;
import org.apache.solr.handler.clustering.ClusteringComponent;
import org.apache.solr.search.DocList;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.RefCounted;
import org.carrot2.util.attribute.AttributeUtils;

/**
 *
 */
@SuppressWarnings("unchecked")
public class CarrotClusteringEngineTest extends AbstractClusteringTest {
	public void testCarrotLingo() throws Exception {
		checkEngine(getClusteringEngine("default"), 9);
	}

	public void testCarrotStc() throws Exception {
		checkEngine(getClusteringEngine("stc"), 2);
	}

	public void testWithoutSubclusters() throws Exception {
		checkClusters(checkEngine(getClusteringEngine("mock"), this.numberOfDocs),
				1, 1, 0);
	}

	public void testWithSubclusters() throws Exception {
		ModifiableSolrParams params = new ModifiableSolrParams();
		params.set(CarrotParams.OUTPUT_SUB_CLUSTERS, true);
		checkClusters(checkEngine(getClusteringEngine("mock"), this.numberOfDocs,
				params), 1, 1, 2);
	}
	
	public void testNumDescriptions() throws Exception {
		ModifiableSolrParams params = new ModifiableSolrParams();
		params.set(AttributeUtils.getKey(MockClusteringAlgorithm.class, "labels"), 5);
		params.set(CarrotParams.NUM_DESCRIPTIONS, 3);
		checkClusters(checkEngine(getClusteringEngine("mock"), this.numberOfDocs,
				params), 1, 3, 0);
	}
	
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

	private NamedList checkEngine(CarrotClusteringEngine engine,
			int expectedNumClusters) throws IOException {
		return checkEngine(engine, expectedNumClusters, new ModifiableSolrParams());
	}

	private NamedList checkEngine(CarrotClusteringEngine engine,
			int expectedNumClusters, SolrParams clusteringParams) throws IOException {
		// Get all documents to cluster
		RefCounted<SolrIndexSearcher> ref = h.getCore().getSearcher();
		MatchAllDocsQuery query = new MatchAllDocsQuery();
		DocList docList;
		try {
			SolrIndexSearcher searcher = ref.get();
			docList = searcher.getDocList(query, (Query) null, new Sort(), 0,
					numberOfDocs);
			assertEquals("docList size", this.numberOfDocs, docList.matches());
		} finally {
			ref.decref();
		}

		ModifiableSolrParams solrParams = new ModifiableSolrParams();
		solrParams.add(CarrotParams.PRODUCE_SUMMARY, "true");
		solrParams.add(clusteringParams);

		// Perform clustering
		NamedList results = engine.cluster(query, docList, solrParams);
		assertEquals("number of clusters", expectedNumClusters, results.size());
		checkClusters(results, false);
		return results;
	}

	private void checkClusters(NamedList results, int expectedDocCount,
			int expectedLabelCount, int expectedSubclusterCount) {
		for (int i = 0; i < results.size(); i++) {
			if (results.getName(i).equals("cluster")) {
				NamedList cluster = (NamedList) results.getVal(i);
				checkCluster(cluster, expectedDocCount, expectedLabelCount,
						expectedSubclusterCount);
			}
		}
	}

	private void checkClusters(NamedList results, boolean hasSubclusters) {
		for (int i = 0; i < results.size(); i++) {
			if (results.getName(i).equals("cluster")) {
				NamedList cluster = (NamedList) results.getVal(i);
				checkCluster(cluster, hasSubclusters);
			}
		}
	}

	private void checkCluster(NamedList cluster, boolean hasSubclusters) {
		NamedList docs = (NamedList) cluster.get("docs");
		assertNotNull("docs is null and it shouldn't be", docs);
		for (int j = 0; j < docs.size(); j++) {
			String id = (String) docs.getVal(j);
			assertNotNull("id is null and it shouldn't be", id);
		}

		NamedList labels = (NamedList) cluster.get("labels");
		assertNotNull("labels is null but it shouldn't be", labels);

		if (hasSubclusters) {
			NamedList subclusters = (NamedList) cluster.get("clusters");
			assertNotNull("subclusters is null but it shouldn't be", subclusters);
		}
	}

	private void checkCluster(NamedList cluster, int expectedDocCount,
			int expectedLabelCount, int expectedSubclusterCount) {
		checkCluster(cluster, expectedSubclusterCount > 0);
		assertEquals("number of docs in cluster", expectedDocCount,
				((NamedList) cluster.get("docs")).size());
		assertEquals("number of labels in cluster", expectedLabelCount,
				((NamedList) cluster.get("labels")).size());

		if (expectedSubclusterCount > 0) {
			NamedList subclusters = (NamedList) cluster.get("clusters");
			assertEquals("numClusters", expectedSubclusterCount, subclusters.size());
			assertEquals("number of subclusters in cluster",
					expectedSubclusterCount, subclusters.size());
		}
	}
}
