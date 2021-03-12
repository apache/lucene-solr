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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest.CreateAlias;
import org.apache.solr.client.solrj.request.CollectionAdminRequest.CreateShard;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrException;
import org.junit.Test;

/**
 * Unit tests for {@link CollectionAdminRequest}.
 */
public class TestCollectionAdminRequest extends SolrTestCase {
  
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

  @Test
  public void testDeleteBackupsV2Mapping() {
    final V2Request deleteBackupRequest = getV2Request(CollectionAdminRequest.deleteBackupById("someBackupName", 1));

    assertEquals(SolrRequest.METHOD.POST, deleteBackupRequest.getMethod());
    assertEquals("/collections/backups", deleteBackupRequest.getPath());
    assertEquals("{\"delete-backups\":{\"name\":\"someBackupName\",\"backupId\":1}}",
        getRequestBody(deleteBackupRequest, ClientUtils.TEXT_JSON));
  }

  @Test
  public void testListBackupsV2Mapping() {
    final V2Request listBackupRequest = getV2Request(CollectionAdminRequest.listBackup("backupName"));

    assertEquals(SolrRequest.METHOD.POST, listBackupRequest.getMethod());
    assertEquals("/collections/backups", listBackupRequest.getPath());
    assertEquals("{\"list-backups\":{\"name\":\"backupName\"}}",
        getRequestBody(listBackupRequest, ClientUtils.TEXT_JSON));
  }

  private V2Request getV2Request(CollectionAdminRequest request) {
    request.setUseV2(true);
    return (V2Request) request.getV2Request();
  }

  private String getRequestBody(V2Request request, String contentType) {
    final ByteArrayOutputStream os = new ByteArrayOutputStream();
    try {
      request.getContentWriter(contentType).write(os);
      return new String(os.toByteArray(), StandardCharsets.UTF_8);

    } catch (IOException e) {
      /* Unreachable in practice, since we're not doing any I/O here */
      throw new RuntimeException(e);
    }
  }
}
