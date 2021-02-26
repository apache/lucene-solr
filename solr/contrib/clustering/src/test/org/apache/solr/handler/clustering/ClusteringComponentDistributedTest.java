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
package org.apache.solr.handler.clustering;

import org.apache.solr.BaseDistributedSearchTestCase;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.response.Cluster;
import org.apache.solr.client.solrj.response.ClusteringResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@SuppressSSL
public class ClusteringComponentDistributedTest extends BaseDistributedSearchTestCase {
  private final static String QUERY_TESTSET_SAMPLE_DOCUMENTS = "testSet:sampleDocs";

  @Override
  public String getSolrHome() {
    return getFile("clustering/solr/collection1").getParent();
  }

  @Before
  public void indexDocs() throws Exception {
    del("*:*");

    String[] languages = {
        "English",
        "French",
        "German",
        "Unknown",
    };

    int docId = 0;
    for (String[] doc : SampleData.SAMPLE_DOCUMENTS) {
      index(
          "id", Integer.toString(docId),
          "title", doc[0],
          "snippet", doc[1],
          "testSet", "sampleDocs",
          "lang", languages[docId % languages.length]
      );
      docId++;
    }
    commit();
  }

  @Test
  @ShardsFixed(num = 2)
  public void testLingoAlgorithm() throws Exception {
    compareToExpected(clusters(QUERY_TESTSET_SAMPLE_DOCUMENTS, params -> {
      params.add(ClusteringComponent.REQUEST_PARAM_ENGINE, "lingo");
    }));
  }

  @Test
  @ShardsFixed(num = 2)
  public void testStcAlgorithm() throws Exception {
    compareToExpected(clusters(QUERY_TESTSET_SAMPLE_DOCUMENTS, params -> {
      params.add(ClusteringComponent.REQUEST_PARAM_ENGINE, "stc");
    }));
  }

  private void compareToExpected(List<Cluster> actual) throws IOException {
    String resourceSuffix = "";
    String expected = ClusteringComponentTest.getTestResource(getClass(), resourceSuffix);
    ClusteringComponentTest.compareWhitespaceNormalized(toString(actual), expected);
  }

  private List<Cluster> clusters(String query, Consumer<ModifiableSolrParams> paramsConsumer) throws Exception {
    handle.clear();
    handle.put("responseHeader", SKIP);
    handle.put("response", SKIP);

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.add(CommonParams.Q, query);
    params.add(CommonParams.ROWS, "1000");
    params.add(CommonParams.SORT, id + " desc");
    params.add(ClusteringComponent.COMPONENT_NAME, "true");
    paramsConsumer.accept(params);

    QueryResponse response = query(true, params);

    ClusteringResponse clusteringResponse = response.getClusteringResponse();
    Assert.assertNotNull(clusteringResponse);

    return clusteringResponse.getClusters();
  }

  private String toString(List<Cluster> clusters) {
    return toString(clusters, "", new StringBuilder()).toString();
  }

  private StringBuilder toString(List<Cluster> clusters, String indent, StringBuilder sb) {
    clusters.forEach(c -> {
      sb.append(indent);
      sb.append("- " + c.getLabels().stream().collect(Collectors.joining("; ")));
      if (!c.getDocs().isEmpty()) {
        sb.append(" [" + c.getDocs().size() + "]");
      }
      sb.append("\n");

      if (!c.getClusters().isEmpty()) {
        toString(c.getClusters(), indent + "  ", sb);
      }
    });
    return sb;
  }
}
