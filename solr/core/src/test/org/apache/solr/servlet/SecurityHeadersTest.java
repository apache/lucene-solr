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
package org.apache.solr.servlet;

import java.net.URI;
import java.util.Arrays;
import java.util.Map;

import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.params.SolrParams;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;

import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Confirm that the expected security headers are returned when making requests to solr,
 * regardless of wether the request is interanlly forwared to another node.
 */
@org.apache.lucene.util.LuceneTestCase.AwaitsFix(bugUrl="https://issues.apache.org/jira/browse/SOLR-14903")
public class SecurityHeadersTest extends SolrCloudTestCase {

  private static final String COLLECTION = "xxx" ;

  private static final int NODE_COUNT = 2;

  /* A quick and dirty mapping of the headers/values we expect to find */
  private static final SolrParams EXPECTED_HEADERS
    = params("Content-Security-Policy", "default-src 'none'; base-uri 'none'; connect-src 'self'; form-action 'self'; font-src 'self'; frame-ancestors 'none'; img-src 'self'; media-src 'self'; style-src 'self' 'unsafe-inline'; script-src 'self'; worker-src 'self';",
             "X-Content-Type-Options", "nosniff",
             "X-Frame-Options", "SAMEORIGIN",
             "X-XSS-Protection", "1; mode=block");
  
  @BeforeClass
  public static void setupCluster() throws Exception {

    configureCluster(NODE_COUNT).configure();

    // create a 1 shard x 1 node collection
    CollectionAdminRequest.createCollection(COLLECTION, null, 1, 1)
        .process(cluster.getSolrClient());

  }

  @Test
  public void testHeaders() throws Exception {
    // it shouldn't matter what node our lone replica/core wound up on, headers should be the same...
    for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
      try (SolrClient solrClient = jetty.newClient()) {
        final HttpClient client = ((HttpSolrClient) solrClient).getHttpClient();

        // path shouldn't matter -- even if bogus / 404
        for (String path : Arrays.asList("/select", "/bogus")) {
          final HttpResponse resp = client.execute
            (new HttpGet(URI.create(jetty.getBaseUrl().toString() + "/" + COLLECTION + path)));

          for (Map.Entry<String,String[]> entry : EXPECTED_HEADERS) {
            // these exact arrays (of 1 element each) should be *ALL* of the header instances...
            // no more, no less.
            assertEquals(entry.getValue(),
                         resp.getHeaders(entry.getKey()));
            
          }
        }
      }
    }
    
  }

  
}

