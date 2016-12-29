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

package org.apache.solr.handler.admin;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test for {@link MetricsHandler}
 */
public class MetricsHandlerTest extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }

  @Test
  public void test() throws Exception {
    MetricsHandler handler = new MetricsHandler(h.getCoreContainer());

    SolrQueryResponse resp = new SolrQueryResponse();
    handler.handleRequestBody(req(CommonParams.QT, "/admin/metrics", CommonParams.WT, "json"), resp);
    NamedList values = resp.getValues();
    assertNotNull(values.get("metrics"));
    values = (NamedList) values.get("metrics");
    System.out.println(values);
    assertNotNull(values.get("solr.jetty"));
    assertNotNull(values.get("solr.jvm"));
    assertNotNull(values.get("solr.http"));
    assertNotNull(values.get("solr.node"));
    NamedList nl = (NamedList) values.get("solr.core.collection1");
    assertNotNull(nl);
    assertNotNull(nl.get("newSearcherErrors")); // counter type
    assertNotNull(((NamedList) nl.get("newSearcherErrors")).get("requests"));
    assertEquals(0L, ((NamedList) nl.get("newSearcherErrors")).get("requests"));
    nl = (NamedList) values.get("solr.node");
    assertNotNull(nl.get("cores.loaded")); // int gauge
    assertEquals(1, ((NamedList) nl.get("cores.loaded")).get("value"));
    assertNotNull(nl.get("QUERYHANDLER./admin/authorization.clientErrors")); // timer type
    assertEquals(5, ((NamedList) nl.get("QUERYHANDLER./admin/authorization.clientErrors")).size());

    resp = new SolrQueryResponse();
    handler.handleRequestBody(req(CommonParams.QT, "/admin/metrics", CommonParams.WT, "json", "group", "jvm,jetty"), resp);
    values = resp.getValues();
    assertNotNull(values.get("metrics"));
    values = (NamedList) values.get("metrics");
    assertEquals(2, values.size());
    assertNotNull(values.get("solr.jetty"));
    assertNotNull(values.get("solr.jvm"));

    resp = new SolrQueryResponse();
    handler.handleRequestBody(req(CommonParams.QT, "/admin/metrics", CommonParams.WT, "json", "group", "jvm,jetty"), resp);
    values = resp.getValues();
    assertNotNull(values.get("metrics"));
    values = (NamedList) values.get("metrics");
    assertEquals(2, values.size());
    assertNotNull(values.get("solr.jetty"));
    assertNotNull(values.get("solr.jvm"));

    resp = new SolrQueryResponse();
    handler.handleRequestBody(req(CommonParams.QT, "/admin/metrics", CommonParams.WT, "json", "group", "jvm", "group", "jetty"), resp);
    values = resp.getValues();
    assertNotNull(values.get("metrics"));
    values = (NamedList) values.get("metrics");
    assertEquals(2, values.size());
    assertNotNull(values.get("solr.jetty"));
    assertNotNull(values.get("solr.jvm"));

    resp = new SolrQueryResponse();
    handler.handleRequestBody(req(CommonParams.QT, "/admin/metrics", CommonParams.WT, "json", "group", "node", "type", "counter"), resp);
    values = resp.getValues();
    assertNotNull(values.get("metrics"));
    values = (NamedList) values.get("metrics");
    assertEquals(1, values.size());
    assertNotNull(values.get("solr.node"));
    assertNull(values.get("QUERYHANDLER./admin/authorization.errors")); // this is a timer node
  }
}
