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

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.impl.CloudSolrClient.Builder;
import org.junit.Test;

public class CloudSolrClientBuilderTest extends LuceneTestCase {
  private static final String ANY_CHROOT = "/ANY_CHROOT";
  private static final String ANY_ZK_HOST = "ANY_ZK_HOST";
  private static final String ANY_OTHER_ZK_HOST = "ANY_OTHER_ZK_HOST";

  @Test(expected = IllegalArgumentException.class)
  public void testNoZkHostSpecified() {
    new Builder()
      .withZkChroot(ANY_CHROOT)
      .build();
  }
  
  @Test
  public void testSingleZkHostSpecified() throws IOException {
    try(CloudSolrClient createdClient = new Builder()
        .withZkHost(ANY_ZK_HOST)
        .withZkChroot(ANY_CHROOT)
        .build()) {
      final String clientZkHost = createdClient.getZkHost();
    
      assertTrue(clientZkHost.contains(ANY_ZK_HOST));
    }
  }
  
  @Test
  public void testSeveralZkHostsSpecifiedSingly() throws IOException {
    try (CloudSolrClient createdClient = new Builder()
        .withZkHost(ANY_ZK_HOST)
        .withZkHost(ANY_OTHER_ZK_HOST)
        .withZkChroot(ANY_CHROOT)
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
    try(CloudSolrClient createdClient = new Builder()
        .withZkHost(zkHosts)
        .withZkChroot(ANY_CHROOT)
        .build()) {
      final String clientZkHost = createdClient.getZkHost();
    
      assertTrue(clientZkHost.contains(ANY_ZK_HOST));
      assertTrue(clientZkHost.contains(ANY_OTHER_ZK_HOST));
    }
  }
  
  @Test
  public void testByDefaultConfiguresClientToSendUpdatesOnlyToShardLeaders() throws IOException {
    try(CloudSolrClient createdClient = new Builder()
        .withZkHost(ANY_ZK_HOST)
        .withZkChroot(ANY_CHROOT)
        .build()) {
      assertTrue(createdClient.isUpdatesToLeaders() == true);
    }
  }

  @Test
  public void testIsDirectUpdatesToLeadersOnlyDefault() throws IOException {
    try(CloudSolrClient createdClient = new Builder()
        .withZkHost(ANY_ZK_HOST)
        .withZkChroot(ANY_CHROOT)
        .build()) {
      assertFalse(createdClient.isDirectUpdatesToLeadersOnly());
    }
  }
}
