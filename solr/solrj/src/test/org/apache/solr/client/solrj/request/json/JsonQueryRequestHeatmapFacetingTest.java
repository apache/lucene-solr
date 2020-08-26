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

package org.apache.solr.client.solrj.request.json;


import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.client.solrj.response.json.HeatmapJsonFacet;
import org.apache.solr.client.solrj.response.json.NestableJsonFacet;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.util.ExternalPaths;
import org.junit.BeforeClass;
import org.junit.Test;

public class JsonQueryRequestHeatmapFacetingTest extends SolrCloudTestCase {

  private static final String COLLECTION_NAME = "spatialdata";
  private static final String CONFIG_NAME = "spatialdata_config";
  private static final String FIELD = "location_srpt";

  @BeforeClass
  public static void beforeJsonQueryRequestHeatmapFacetingTest() throws Exception {
    configureCluster(1)
        .addConfig(CONFIG_NAME, new File(ExternalPaths.SOURCE_HOME, "solrj/src/test-files/solrj/solr/configsets/spatial/conf").toPath())
        .configure();

    final List<String> solrUrls = new ArrayList<>();
    solrUrls.add(cluster.getJettySolrRunner(0).getBaseUrl());

    CollectionAdminRequest.createCollection(COLLECTION_NAME, CONFIG_NAME, 1, 1).process(cluster.getSolrClient());

    indexSpatialData();
  }

  private static void indexSpatialData() throws Exception {
    final SolrInputDocument doc1 = new SolrInputDocument("id", "0", FIELD, "ENVELOPE(100, 120, 80, 40)");
    final SolrInputDocument doc2 = new SolrInputDocument("id", "1", FIELD, "ENVELOPE(-120, -110, 80, 20)");
    final SolrInputDocument doc3 = new SolrInputDocument("id", "3", FIELD, "POINT(70 60)");
    final SolrInputDocument doc4 = new SolrInputDocument("id", "4", FIELD, "POINT(91 89)");
    final List<SolrInputDocument> docs = Arrays.asList(doc1, doc2, doc3, doc4);

    UpdateResponse updateResponse = cluster.getSolrClient().add(COLLECTION_NAME, docs);
    assertTrue(updateResponse.getStatus() == 0);

    updateResponse = cluster.getSolrClient().commit(COLLECTION_NAME);
    assertTrue(updateResponse.getStatus() == 0);

    QueryResponse queryResponse = cluster.getSolrClient().query(COLLECTION_NAME, new SolrQuery("*:*"));
    assertTrue("Expected "+docs.size()+" docs in "+COLLECTION_NAME,
        queryResponse.getResults().getNumFound() == docs.size());
  }

  @Test
  public void testHeatmapFacet() throws Exception {
    final List<List<Integer>> expectedHeatmapGrid = Arrays.asList(
        Arrays.asList(0, 0, 2, 1, 0, 0),
        Arrays.asList(0, 0, 1, 1, 0, 0),
        Arrays.asList(0, 1, 1, 1, 0, 0),
        Arrays.asList(0, 0, 1, 1, 0, 0),
        Arrays.asList(0, 0, 1, 1, 0, 0),
        null,
        null
    );
    final JsonQueryRequest request = new JsonQueryRequest()
        .setQuery("*:*")
        .setLimit(0)
        .withFacet("heatmapFacet", new HeatmapFacetMap(FIELD)
            .setHeatmapFormat(HeatmapFacetMap.HeatmapFormat.INTS2D)
            .setRegionQuery("[\"50 20\" TO \"180 90\"]")
            .setGridLevel(4)
        );

    QueryResponse response = request.process(cluster.getSolrClient(), COLLECTION_NAME);
    final NestableJsonFacet topLevelFacet = response.getJsonFacetingResponse();
    final HeatmapJsonFacet heatmap = topLevelFacet.getHeatmapFacetByName("heatmapFacet");
    final List<List<Integer>> actualHeatmapGrid = heatmap.getCountGrid();
    assertEquals(expectedHeatmapGrid, actualHeatmapGrid);
  }
}
