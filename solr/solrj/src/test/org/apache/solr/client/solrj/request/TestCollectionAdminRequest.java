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
package org.apache.solr.client.solrj.request;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.request.CollectionAdminRequest.CreateAlias;
import org.apache.solr.client.solrj.request.CollectionAdminRequest.CreateShard;
import org.apache.solr.common.SolrException;
import org.junit.Test;

/**
 * Unit tests for {@link CollectionAdminRequest}.
 */
public class TestCollectionAdminRequest extends LuceneTestCase {
  
  @Test
  // commented out on: 24-Dec-2018   @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // added 20-Sep-2018
  public void testInvalidCollectionNameRejectedWhenCreatingCollection() {
    final SolrException e = expectThrows(SolrException.class, () -> {
        CollectionAdminRequest.createCollection("invalid$collection@name", null, 1, 1);
      });
    final String exceptionMessage = e.getMessage();
    assertTrue(exceptionMessage.contains("Invalid collection"));
    assertTrue(exceptionMessage.contains("invalid$collection@name"));
    assertTrue(exceptionMessage.contains("must consist entirely of periods, underscores, hyphens, and alphanumerics"));
  }
  
  @Test
  // commented out on: 24-Dec-2018   @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // added 20-Sep-2018
  public void testInvalidShardNamesRejectedWhenCreatingImplicitCollection() {
    final SolrException e = expectThrows(SolrException.class, () -> {
        CollectionAdminRequest.createCollectionWithImplicitRouter("fine", "fine", "invalid$shard@name",1,0,0);
      });
    final String exceptionMessage = e.getMessage();
    assertTrue(exceptionMessage.contains("Invalid shard"));
    assertTrue(exceptionMessage.contains("invalid$shard@name"));
    assertTrue(exceptionMessage.contains("must consist entirely of periods, underscores, hyphens, and alphanumerics"));
  }
  
  @Test
  // commented out on: 24-Dec-2018   @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // added 20-Sep-2018
  public void testInvalidShardNamesRejectedWhenCallingSetShards() {
    CollectionAdminRequest.Create request = CollectionAdminRequest.createCollectionWithImplicitRouter("fine",null,"fine",1);
    final SolrException e = expectThrows(SolrException.class, () -> {
        request.setShards("invalid$shard@name");
      });
    final String exceptionMessage = e.getMessage();
    assertTrue(exceptionMessage.contains("Invalid shard"));
    assertTrue(exceptionMessage.contains("invalid$shard@name"));
    assertTrue(exceptionMessage.contains("must consist entirely of periods, underscores, hyphens, and alphanumerics"));
  }
  
  @Test
  // commented out on: 24-Dec-2018   @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // added 20-Sep-2018
  public void testInvalidAliasNameRejectedWhenCreatingAlias() {
    final SolrException e = expectThrows(SolrException.class, () -> {
        CreateAlias createAliasRequest = CollectionAdminRequest.createAlias("invalid$alias@name","ignored");
      });
    final String exceptionMessage = e.getMessage();
    assertTrue(exceptionMessage.contains("Invalid alias"));
    assertTrue(exceptionMessage.contains("invalid$alias@name"));
    assertTrue(exceptionMessage.contains("must consist entirely of periods, underscores, hyphens, and alphanumerics"));
  }
  
  @Test
  // commented out on: 24-Dec-2018   @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // added 20-Sep-2018
  public void testInvalidShardNameRejectedWhenCreatingShard() {
    final SolrException e = expectThrows(SolrException.class, () -> {
        CreateShard createShardRequest = CollectionAdminRequest.createShard("ignored","invalid$shard@name");
      });
    final String exceptionMessage = e.getMessage();
    assertTrue(exceptionMessage.contains("Invalid shard"));
    assertTrue(exceptionMessage.contains("invalid$shard@name"));
    assertTrue(exceptionMessage.contains("must consist entirely of periods, underscores, hyphens, and alphanumerics"));
  }
}
