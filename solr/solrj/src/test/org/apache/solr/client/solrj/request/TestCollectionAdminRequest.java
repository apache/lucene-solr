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
import org.apache.solr.client.solrj.request.CollectionAdminRequest.Create;
import org.apache.solr.client.solrj.request.CollectionAdminRequest.CreateAlias;
import org.apache.solr.client.solrj.request.CollectionAdminRequest.CreateShard;
import org.junit.Test;

/**
 * Unit tests for {@link CollectionAdminRequest}.
 */
public class TestCollectionAdminRequest extends LuceneTestCase {
  
  @Test
  public void testInvalidCollectionNameRejectedWhenCreatingCollection() {
    final Create createRequest = new Create();
    try {
      createRequest.setCollectionName("invalid$collection@name");
      fail();
    } catch (IllegalArgumentException e) {
      final String exceptionMessage = e.getMessage();
      assertTrue(exceptionMessage.contains("Invalid collection"));
      assertTrue(exceptionMessage.contains("invalid$collection@name"));
      assertTrue(exceptionMessage.contains("must consist entirely of periods, underscores, and alphanumerics"));
    }
  }
  
  @Test
  public void testInvalidShardNamesRejectedWhenCreatingCollection() {
    final Create createRequest = new Create();
    try {
      createRequest.setShards("invalid$shard@name");
      fail();
    } catch (IllegalArgumentException e) {
      final String exceptionMessage = e.getMessage();
      assertTrue(exceptionMessage.contains("Invalid shard"));
      assertTrue(exceptionMessage.contains("invalid$shard@name"));
      assertTrue(exceptionMessage.contains("must consist entirely of periods, underscores, and alphanumerics"));
    }
  }
  
  @Test
  public void testInvalidAliasNameRejectedWhenCreatingAlias() {
    final CreateAlias createAliasRequest = new CreateAlias();
    try {
      createAliasRequest.setAliasName("invalid$alias@name");
      fail();
    } catch (IllegalArgumentException e) {
      final String exceptionMessage = e.getMessage();
      assertTrue(exceptionMessage.contains("Invalid collection"));
      assertTrue(exceptionMessage.contains("invalid$alias@name"));
      assertTrue(exceptionMessage.contains("must consist entirely of periods, underscores, and alphanumerics"));
    }
  }
  
  @Test
  public void testInvalidShardNameRejectedWhenCreatingShard() {
    final CreateShard createShardRequest = new CreateShard();
    try {
      createShardRequest.setShardName("invalid$shard@name");
      fail();
    } catch (IllegalArgumentException e) {
      final String exceptionMessage = e.getMessage();
      assertTrue(exceptionMessage.contains("Invalid shard"));
      assertTrue(exceptionMessage.contains("invalid$shard@name"));
      assertTrue(exceptionMessage.contains("must consist entirely of periods, underscores, and alphanumerics"));
    }
  }
}
