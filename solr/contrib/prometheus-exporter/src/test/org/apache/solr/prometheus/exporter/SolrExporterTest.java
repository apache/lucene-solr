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
package org.apache.solr.prometheus.exporter;

import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.ContentStreamUpdateRequest;
import org.junit.Test;

import java.io.File;
import java.net.ServerSocket;
import java.net.URI;
import java.util.Arrays;
import java.util.List;

/**
 * Unit test for SolrExporter.
 */
@Slow
public class SolrExporterTest extends SolrExporterTestBase {

  @Override
  public void setUp() throws Exception {
      super.setUp();
  }

  @Override
  public void tearDown() throws Exception {
      super.tearDown();
  }

  @Test
  public void testExecute() throws Exception {
    // solr client
    CloudSolrClient cloudSolrClient = cluster.getSolrClient();

    int port;
    ServerSocket socket = null;
    try {
      socket = new ServerSocket(0);
      port = socket.getLocalPort();
    } finally {
      socket.close();
    }

    // index sample docs
    File exampleDocsDir = new File(getFile("exampledocs").getAbsolutePath());
    List<File> xmlFiles = Arrays.asList(exampleDocsDir.listFiles((dir, name) -> name.endsWith(".xml")));
    for (File xml : xmlFiles) {
      ContentStreamUpdateRequest req = new ContentStreamUpdateRequest("/update");
      req.addFile(xml, "application/xml");
      cloudSolrClient.request(req, "collection1");
    }
    cloudSolrClient.commit("collection1");

    // start exporter
    SolrExporter solrExporter = new SolrExporter(port, cloudSolrClient, getFile("conf/solr-exporter-config.xml").toPath(), 1);
    try {
      solrExporter.start();

      URI uri = new URI("http://localhost:" + String.valueOf(port) + "/metrics");

      CloseableHttpClient httpclient = HttpClients.createDefault();
      CloseableHttpResponse response = null;
      try {
        HttpGet request = new HttpGet(uri);
        response = httpclient.execute(request);

        int expectedHTTPStatusCode = HttpStatus.SC_OK;
        int actualHTTPStatusCode = response.getStatusLine().getStatusCode();
        assertEquals(expectedHTTPStatusCode, actualHTTPStatusCode);
      } finally {
        response.close();
        httpclient.close();
      }
    } finally {
      solrExporter.stop();
    }
  }
}
