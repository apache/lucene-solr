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
package org.apache.solr.client.solrj.response;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import org.apache.solr.SolrJettyTestBase;
import org.apache.solr.client.solrj.impl.XMLResponseParser;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrResourceLoader;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test for ClusteringComponent's response in Solrj
 */
public class TestClusteringResponse extends SolrJettyTestBase {

  @Test
  public void testClusteringResponse() throws Exception {
    XMLResponseParser parser = new XMLResponseParser();
    /*Load a simple XML with the clustering response encoded in an XML format*/
    InputStream is = new SolrResourceLoader().openResource("solrj/sampleClusteringResponse.xml");
    assertNotNull(is);
    Reader in = new InputStreamReader(is, StandardCharsets.UTF_8);
    NamedList<Object> response = parser.processResponse(in);
    in.close();

    QueryResponse qr = new QueryResponse(response, null);
    ClusteringResponse clusteringResponse = qr.getClusteringResponse();
    List<Cluster> clusters = clusteringResponse.getClusters();
    Assert.assertEquals(4, clusters.size());

    checkCluster(clusters.get(0), Arrays.asList("label1"), Arrays.asList("id1", "id2", "id3"), 0.6d, false);
    checkCluster(clusters.get(1), Arrays.asList("label2"), Arrays.asList("id5", "id6"), 0.93d, false);
    checkCluster(clusters.get(2), Arrays.asList("label3"), Arrays.asList("id7", "id8"), 1.26d, false);
    checkCluster(clusters.get(3), Arrays.asList("label4"), Arrays.asList("id9"), 0d, true);
    
    List<Cluster> sub = clusters.get(0).getSubclusters();
    checkCluster(sub.get(0), Arrays.asList("label1.sub1"), Arrays.asList("id1", "id2"), 0.0d, false);
    checkCluster(sub.get(1), Arrays.asList("label1.sub2"), Arrays.asList("id2"), 0.0d, false);
    assertEquals(sub.size(), 2);
  }

  private void checkCluster(Cluster cluster, List<String> labels, List<String> docRefs, double score, boolean otherTopics) {
    Assert.assertEquals(cluster.getLabels(), labels);
    Assert.assertEquals(cluster.getDocs(), docRefs);
    Assert.assertTrue(Double.compare(cluster.getScore(), score) == 0);
    Assert.assertEquals(otherTopics, cluster.isOtherTopics());
  }
}
