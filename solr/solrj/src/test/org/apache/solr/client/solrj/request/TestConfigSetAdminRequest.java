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

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.response.ConfigSetAdminResponse;
import org.junit.Test;

/**
 * Basic error checking of ConfigSetAdminRequests.
 */
public class TestConfigSetAdminRequest extends SolrTestCaseJ4 {

  @Test
  public void testNoAction() {
    ConfigSetAdminRequest request = new MyConfigSetAdminRequest();
    verifyException(request, "action");
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

  private void verifyException(ConfigSetAdminRequest request, String errorContains) {
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
