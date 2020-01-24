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

package org.apache.solr.handler.component;

import java.util.ArrayList;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.BeforeClass;

public class ResponseBuilderTest extends SolrTestCaseJ4 {

  SolrQueryRequest req = req("q", "title:test");
  SolrQueryResponse rsp = new SolrQueryResponse();

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }

  //This test is being added to verify responseBuilder.isDistributed() exists and is visible.
  public void testIsDistrib(){
    ResponseBuilder responseBuilder = new ResponseBuilder(req, rsp, new ArrayList<>(0));
    assertFalse(responseBuilder.isDistributed());
  }

  public void testDoAnalyticsAccessors() {
    ResponseBuilder rb = new ResponseBuilder(req, rsp, new ArrayList<>(0));
    assertFalse(rb.isAnalytics());
    rb.setAnalytics(true);
    assertTrue(rb.isAnalytics());
    rb.setAnalytics(false);
    assertFalse(rb.isAnalytics());
  }

  public void testIsOlapAnalyticsAccessors() {
    ResponseBuilder rb = new ResponseBuilder(req, rsp, new ArrayList<>(0));
    assertFalse(rb.isOlapAnalytics());
    rb.setOlapAnalytics(true);
    assertTrue(rb.isOlapAnalytics());
    rb.setOlapAnalytics(false);
    assertFalse(rb.isOlapAnalytics());
  }

  public void testAnalyticsRequestManagerAccessors() {
    ResponseBuilder rb = new ResponseBuilder(req, rsp, new ArrayList<>(0));
    assertNull(rb.getAnalyticsRequestManager());
    rb.setAnalyticsRequestManager(this);
    assertNotNull(rb.getAnalyticsRequestManager());
    assertEquals(rb.getAnalyticsRequestManager(), this);
  }
}
