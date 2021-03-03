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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.solr.SolrTestCase;
import org.junit.Test;
import org.mockito.Mockito;

import static org.apache.solr.SolrTestCaseJ4.assumeWorkingMockito;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CloudHttp2SolrClientBuilderTest extends SolrTestCase {
  private static final String ANY_CHROOT = "/ANY_CHROOT";
  private static final String ANY_ZK_HOST = "ANY_ZK_HOST";
  private static final String ANY_OTHER_ZK_HOST = "ANY_OTHER_ZK_HOST";

  @Test
  public void testSingleZkHostSpecified() throws IOException {
    try(CloudHttp2SolrClient createdClient = new CloudHttp2SolrClient.Builder(Collections.singletonList(ANY_ZK_HOST), Optional.of(ANY_CHROOT))
        .build()) {
      final String clientZkHost = createdClient.getZkHost();

      assertTrue(clientZkHost.contains(ANY_ZK_HOST));
    }
  }

  @Test
  public void testSeveralZkHostsSpecifiedSingly() throws IOException {
    final List<String> zkHostList = new ArrayList<>();
    zkHostList.add(ANY_ZK_HOST); zkHostList.add(ANY_OTHER_ZK_HOST);
    try (CloudHttp2SolrClient createdClient = new CloudHttp2SolrClient.Builder(zkHostList, Optional.of(ANY_CHROOT))
        .build()) {
      final String clientZkHost = createdClient.getZkHost();

      assertTrue(clientZkHost.contains(ANY_ZK_HOST));
      assertTrue(clientZkHost.contains(ANY_OTHER_ZK_HOST));
    }
  }

  @Test
  public void testSeveralZkHostsSpecifiedTogether() throws IOException {
    final ArrayList<String> zkHosts = new ArrayList<String>();
    zkHosts.add(ANY_ZK_HOST);
    zkHosts.add(ANY_OTHER_ZK_HOST);
    try(CloudHttp2SolrClient createdClient = new CloudHttp2SolrClient.Builder(zkHosts, Optional.of(ANY_CHROOT)).build()) {
      final String clientZkHost = createdClient.getZkHost();

      assertTrue(clientZkHost.contains(ANY_ZK_HOST));
      assertTrue(clientZkHost.contains(ANY_OTHER_ZK_HOST));
    }
  }

  @Test
  public void testByDefaultConfiguresClientToSendUpdatesOnlyToShardLeaders() throws IOException {
    try(CloudHttp2SolrClient createdClient = new CloudHttp2SolrClient.Builder(Collections.singletonList(ANY_ZK_HOST), Optional.of(ANY_CHROOT)).build()) {
      assertTrue(createdClient.isUpdatesToLeaders());
    }
  }

  @Test
  public void testIsDirectUpdatesToLeadersOnlyDefault() throws IOException {
    try(CloudHttp2SolrClient createdClient = new CloudHttp2SolrClient.Builder(Collections.singletonList(ANY_ZK_HOST), Optional.of(ANY_CHROOT)).build()) {
      assertFalse(createdClient.isDirectUpdatesToLeadersOnly());
    }
  }

  @Test
  public void testExternalClientAndInternalBuilderTogether() {
    assumeWorkingMockito();
    expectThrows(IllegalStateException.class, () -> new CloudHttp2SolrClient.Builder(Collections.singletonList(ANY_ZK_HOST), Optional.of(ANY_CHROOT))
            .withHttpClient(Mockito.mock(Http2SolrClient.class))
            .withInternalClientBuilder(Mockito.mock(Http2SolrClient.Builder.class))
            .build());
    expectThrows(IllegalStateException.class, () -> new CloudHttp2SolrClient.Builder(Collections.singletonList(ANY_ZK_HOST), Optional.of(ANY_CHROOT))
            .withInternalClientBuilder(Mockito.mock(Http2SolrClient.Builder.class))
            .withHttpClient(Mockito.mock(Http2SolrClient.class))
            .build());
  }

  @Test
  public void testProvideInternalBuilder() throws IOException {
    assumeWorkingMockito();
    Http2SolrClient http2Client = Mockito.mock(Http2SolrClient.class);
    Http2SolrClient.Builder http2ClientBuilder = Mockito.mock(Http2SolrClient.Builder.class);
    when(http2ClientBuilder.build()).thenReturn(http2Client);
    CloudHttp2SolrClient.Builder clientBuilder = new CloudHttp2SolrClient.Builder(Collections.singletonList(ANY_ZK_HOST), Optional.of(ANY_CHROOT))
            .withInternalClientBuilder(http2ClientBuilder);
    verify(http2ClientBuilder, never()).build();
    try (CloudHttp2SolrClient client = clientBuilder.build()) {
      assertEquals(http2Client, client.getHttpClient());
      verify(http2ClientBuilder, times(1)).build();
      verify(http2Client, never()).close();
    }
    // it's internal, should be closed when closing CloudSolrClient
    verify(http2Client, times(1)).close();
  }

  @Test
  public void testProvideExternalClient() throws IOException {
    assumeWorkingMockito();
    Http2SolrClient http2Client = Mockito.mock(Http2SolrClient.class);
    CloudHttp2SolrClient.Builder clientBuilder = new CloudHttp2SolrClient.Builder(Collections.singletonList(ANY_ZK_HOST), Optional.of(ANY_CHROOT))
            .withHttpClient(http2Client);
    try (CloudHttp2SolrClient client = clientBuilder.build()) {
      assertEquals(http2Client, client.getHttpClient());
    }
    // it's external, should be NOT closed when closing CloudSolrClient
    verify(http2Client, never()).close();
    http2Client.close();
  }

}
