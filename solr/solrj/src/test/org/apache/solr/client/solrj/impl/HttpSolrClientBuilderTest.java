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

package org.apache.solr.client.solrj.impl;

import java.io.IOException;

import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.impl.HttpSolrClient.Builder;
import org.junit.Test;

/**
 * Unit tests for {@link Builder}.
 */
public class HttpSolrClientBuilderTest extends LuceneTestCase {
  private static final String ANY_BASE_SOLR_URL = "ANY_BASE_SOLR_URL";
  private static final HttpClient ANY_HTTP_CLIENT = HttpClientBuilder.create().build();
  private static final ResponseParser ANY_RESPONSE_PARSER = new NoOpResponseParser();
  
  @Test(expected = IllegalArgumentException.class)
  public void testBaseSolrUrlIsRequired() {
    new Builder(null)
        .build();
  }

  @Test
  public void testProvidesBaseSolrUrlToClient() throws IOException {
    try (HttpSolrClient createdClient = new HttpSolrClient.Builder(ANY_BASE_SOLR_URL).build()) {
      assertTrue(createdClient.getBaseURL().equals(ANY_BASE_SOLR_URL));
    }
  }
  
  @Test
  public void testProvidesHttpClientToClient() throws IOException {
    try(HttpSolrClient createdClient = new Builder(ANY_BASE_SOLR_URL)
        .withHttpClient(ANY_HTTP_CLIENT)
        .build()) {
      assertTrue(createdClient.getHttpClient().equals(ANY_HTTP_CLIENT));
    }
  }
  
  @Test
  public void testProvidesResponseParserToClient() throws IOException {
    try(HttpSolrClient createdClient = new Builder(ANY_BASE_SOLR_URL)
        .withResponseParser(ANY_RESPONSE_PARSER)
        .build()) {
      assertTrue(createdClient.getParser().equals(ANY_RESPONSE_PARSER));
    }
  }
  
  @Test
  public void testDefaultsToBinaryResponseParserWhenNoneProvided() throws IOException {
    try(HttpSolrClient createdClient = new Builder(ANY_BASE_SOLR_URL)
        .build()) {
      final ResponseParser usedParser = createdClient.getParser();
      assertTrue(usedParser instanceof BinaryResponseParser);
    }
  }
}
