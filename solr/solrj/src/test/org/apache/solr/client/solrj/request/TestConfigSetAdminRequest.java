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

import java.io.File;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.response.ConfigSetAdminResponse;
import org.apache.solr.common.params.ConfigSetParams;

import org.junit.Test;

/**
 * Basic error checking of ConfigSetAdminRequests.
 */
public class TestConfigSetAdminRequest extends SolrTestCaseJ4 {

  @Test
  public void testNoAction() {
    @SuppressWarnings({"rawtypes"})
    ConfigSetAdminRequest request = new MyConfigSetAdminRequest();
    verifyException(request, "action");
  }

  @Test
  public void testUpload() throws Exception {
    final File tmpFile = createTempFile().toFile();
    ConfigSetAdminRequest.Upload upload = new ConfigSetAdminRequest.Upload();
    verifyException(upload, "ConfigSet");
    
    upload.setConfigSetName("name");
    verifyException(upload, "There must be a ContentStream");
    
    upload.setUploadFile(tmpFile, "application/zip");
    
    assertEquals(1, upload.getContentStreams().size());
    assertEquals("application/zip", upload.getContentStreams().stream().findFirst().get().getContentType());
    
    assertNull(upload.getParams().get(ConfigSetParams.FILE_PATH));
    assertNull(upload.getParams().get(ConfigSetParams.OVERWRITE));
    assertNull(upload.getParams().get(ConfigSetParams.CLEANUP));
    
    upload.setUploadFile(tmpFile, "application/xml")
      .setFilePath("solrconfig.xml")
      .setOverwrite(true);
    
    assertEquals(1, upload.getContentStreams().size());
    assertEquals("application/xml", upload.getContentStreams().stream().findFirst().get().getContentType());
    
    assertEquals("solrconfig.xml", upload.getParams().get(ConfigSetParams.FILE_PATH));
    assertEquals("true", upload.getParams().get(ConfigSetParams.OVERWRITE));
  }
  
  @Test
  public void testCreate() {
    ConfigSetAdminRequest.Create create = new ConfigSetAdminRequest.Create();
    verifyException(create, "ConfigSet");
    create.setConfigSetName("name");
    create.getParams();
  }

  @Test
  public void testDelete() {
    ConfigSetAdminRequest.Delete delete = new ConfigSetAdminRequest.Delete();
    verifyException(delete, "ConfigSet");
  }

  private void verifyException(@SuppressWarnings({"rawtypes"})ConfigSetAdminRequest request, String errorContains) {
    Exception e = expectThrows(Exception.class, request::getParams);
    assertTrue("Expected exception message to contain: " + errorContains,
        e.getMessage().contains(errorContains));
  }

  private static class MyConfigSetAdminRequest extends ConfigSetAdminRequest<MyConfigSetAdminRequest, ConfigSetAdminResponse> {
      public MyConfigSetAdminRequest() {}

      @Override
      public MyConfigSetAdminRequest getThis() {
        return this;
      }

      @Override
      public ConfigSetAdminResponse createResponse(SolrClient client) {
        return new ConfigSetAdminResponse();
      }
    };
}
