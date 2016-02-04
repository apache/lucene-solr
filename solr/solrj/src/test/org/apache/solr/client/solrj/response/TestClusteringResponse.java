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
import java.util.LinkedList;
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

    //First Cluster
    Cluster cluster1 = clusters.get(0);
    List<String> expectedLabel1 = new LinkedList<String>();
    expectedLabel1.add("label1");
    List<String> expectedDocs1 = new LinkedList<String>();
    expectedDocs1.add("id1");
    expectedDocs1.add("id2");
    expectedDocs1.add("id3");
    Assert.assertEquals(expectedLabel1, cluster1.getLabels());
    Assert.assertEquals(expectedDocs1, cluster1.getDocs());
    Assert.assertEquals(expectedLabel1, cluster1.getLabels());
    Assert.assertEquals(0.6, cluster1.getScore(), 0);
    //Second Cluster
    Cluster cluster2 = clusters.get(1);
    List<String> expectedLabel2 = new LinkedList<String>();
    expectedLabel2.add("label2");
    List<String> expectedDocs2 = new LinkedList<String>();
    expectedDocs2.add("id5");
    expectedDocs2.add("id6");
    Assert.assertEquals(expectedLabel2, cluster2.getLabels());
    Assert.assertEquals(expectedDocs2, cluster2.getDocs());
    Assert.assertEquals(expectedLabel2, cluster2.getLabels());
    Assert.assertEquals(0.93, cluster2.getScore(), 0);
    //Third Cluster
    Cluster cluster3 = clusters.get(2);
    List<String> expectedLabel3 = new LinkedList<String>();
    expectedLabel3.add("label3");
    List<String> expectedDocs3 = new LinkedList<String>();
    expectedDocs3.add("id7");
    expectedDocs3.add("id8");
    Assert.assertEquals(expectedLabel3, cluster3.getLabels());
    Assert.assertEquals(expectedDocs3, cluster3.getDocs());
    Assert.assertEquals(expectedLabel3, cluster3.getLabels());
    Assert.assertEquals(1.26, cluster3.getScore(), 0);
    //Fourth Cluster
    Cluster cluster4 = clusters.get(3);
    List<String> expectedLabel4 = new LinkedList<String>();
    expectedLabel4.add("label4");
    List<String> expectedDocs4 = new LinkedList<String>();
    expectedDocs4.add("id9");
    Assert.assertEquals(expectedLabel4, cluster4.getLabels());
    Assert.assertEquals(expectedDocs4, cluster4.getDocs());
    Assert.assertEquals(expectedLabel4, cluster4.getLabels());
    Assert.assertEquals(0.0, cluster4.getScore(), 0);

  }

}
