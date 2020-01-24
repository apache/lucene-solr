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

import org.apache.solr.SolrTestCase;
import org.junit.Test;

/**
 * Test for DelegationTokenRequests
 */
public class TestDelegationTokenRequest extends SolrTestCase {

  @Test
  public void testGetRequest() throws Exception {
    // without renewer
    DelegationTokenRequest.Get get = new DelegationTokenRequest.Get();
    assertEquals("GETDELEGATIONTOKEN", get.getParams().get("op"));
    assertNull(get.getParams().get("renewer"));


    // with renewer
    final String renewer = "test";
    get = new DelegationTokenRequest.Get(renewer);
    assertEquals("GETDELEGATIONTOKEN", get.getParams().get("op"));
    assertEquals(renewer, get.getParams().get("renewer"));
  }

  @Test
  public void testRenewRequest() throws Exception {
    final String token = "testToken";
    DelegationTokenRequest.Renew renew = new DelegationTokenRequest.Renew(token);
    assertEquals("RENEWDELEGATIONTOKEN", renew.getParams().get("op"));
    assertEquals(token, renew.getParams().get("token"));
    assertTrue(renew.getQueryParams().contains("op"));
    assertTrue(renew.getQueryParams().contains("token"));

    // can handle null token
    renew = new DelegationTokenRequest.Renew(null);
    renew.getParams();
  }

  @Test
  public void testCancelRequest() throws Exception {
    final String token = "testToken";
    DelegationTokenRequest.Cancel cancel = new DelegationTokenRequest.Cancel(token);
    assertEquals("CANCELDELEGATIONTOKEN", cancel.getParams().get("op"));
    assertEquals(token, cancel.getParams().get("token"));
    assertTrue(cancel.getQueryParams().contains("op"));
    assertTrue(cancel.getQueryParams().contains("token"));

    // can handle null token
    cancel = new DelegationTokenRequest.Cancel(null);
    cancel.getParams();
  }
}
