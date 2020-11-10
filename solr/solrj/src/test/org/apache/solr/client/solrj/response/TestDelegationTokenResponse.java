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

package org.apache.solr.client.solrj.response;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;

import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.request.DelegationTokenRequest;
import org.apache.solr.common.SolrException;

import org.junit.Test;

import org.noggit.CharArr;
import org.noggit.JSONWriter;

public class TestDelegationTokenResponse extends SolrTestCase {

  private void delegationTokenResponse(@SuppressWarnings({"rawtypes"})DelegationTokenRequest request,
      DelegationTokenResponse response, String responseBody) throws Exception {
    ResponseParser parser = request.getResponseParser();
    response.setResponse(parser.processResponse(
      IOUtils.toInputStream(responseBody, "UTF-8"), "UTF-8"));
  }

  private String getNestedMapJson(String outerKey, String innerKey, Object innerValue) {
    CharArr out = new CharArr();
    JSONWriter w = new JSONWriter(out, 2);
    Map<String, Object> innerMap = new HashMap<String, Object>();
    innerMap.put(innerKey, innerValue);
    Map<String, Map<String, Object>> outerMap = new HashMap<String, Map<String, Object>>();
    outerMap.put(outerKey, innerMap);
    w.write(outerMap);
    return out.toString();
  }

  private String getMapJson(String key, Object value) {
    CharArr out = new CharArr();
    JSONWriter w = new JSONWriter(out, 2);
    Map<String, Object> map = new HashMap<String, Object>();
    map.put(key, value);
    w.write(map);
    return out.toString();
  }

  @Test
  // commented out on: 24-Dec-2018   @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // added 20-Sep-2018
  public void testGetResponse() throws Exception {
    DelegationTokenRequest.Get getRequest = new DelegationTokenRequest.Get();
    DelegationTokenResponse.Get getResponse = new DelegationTokenResponse.Get();

    // not a map
    expectThrows(SolrException.class, () -> {
      delegationTokenResponse(getRequest, getResponse, "");
      getResponse.getDelegationToken();
    });

    // doesn't have Token outerMap
    final String someToken = "someToken";
    delegationTokenResponse(getRequest, getResponse, getNestedMapJson("NotToken", "urlString", someToken));
    assertNull(getResponse.getDelegationToken());

    // Token is not a map
    delegationTokenResponse(getRequest, getResponse, getMapJson("Token", someToken));
    expectThrows(SolrException.class, getResponse::getDelegationToken);

    // doesn't have urlString
    delegationTokenResponse(getRequest, getResponse, getNestedMapJson("Token", "notUrlString", someToken));
    assertNull(getResponse.getDelegationToken());

    // has Token + urlString
    delegationTokenResponse(getRequest, getResponse, getNestedMapJson("Token", "urlString", someToken));
    assertEquals(someToken, getResponse.getDelegationToken());
  }

  @Test
  // commented out on: 24-Dec-2018   @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // added 20-Sep-2018
  public void testRenewResponse() throws Exception {
    DelegationTokenRequest.Renew renewRequest = new DelegationTokenRequest.Renew("token");
    DelegationTokenResponse.Renew renewResponse = new DelegationTokenResponse.Renew();

    // not a map
    expectThrows(SolrException.class, () -> {
      delegationTokenResponse(renewRequest, renewResponse, "");
      renewResponse.getExpirationTime();
    });

    // doesn't have long
    delegationTokenResponse(renewRequest, renewResponse, getMapJson("notLong", "123"));
    assertNull(renewResponse.getExpirationTime());

    // long isn't valid
    delegationTokenResponse(renewRequest, renewResponse, getMapJson("long", "aaa"));
    expectThrows(SolrException.class, renewResponse::getExpirationTime);

    // valid
    Long expirationTime = Long.MAX_VALUE;
    delegationTokenResponse(renewRequest, renewResponse,
      getMapJson("long", expirationTime));
    assertEquals(expirationTime, renewResponse.getExpirationTime());
  }

  @Test
  public void testCancelResponse() throws Exception {
    // expect empty response
    DelegationTokenRequest.Cancel cancelRequest = new DelegationTokenRequest.Cancel("token");
    DelegationTokenResponse.Cancel cancelResponse = new DelegationTokenResponse.Cancel();
    delegationTokenResponse(cancelRequest, cancelResponse, "");
  }
}
