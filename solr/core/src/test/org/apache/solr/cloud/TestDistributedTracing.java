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

package org.apache.solr.cloud;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import io.opentracing.Span;
import io.opentracing.mock.MockSpan;
import io.opentracing.mock.MockTracer;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.MockTracerConfigurator;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestDistributedTracing extends SolrCloudTestCase {
  private static final String COLLECTION = "collection1";
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Before
  public void beforeTest() throws Exception {
    configureCluster(4)
        .addConfig("config", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .withSolrXml(TEST_PATH().resolve("solr-tracing.xml"))
        .configure();
    CollectionAdminRequest
        .createCollection(COLLECTION, "config", 2, 2)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(COLLECTION, 2, 4);
  }

  @Test
  public void test() throws IOException, SolrServerException {
    CloudSolrClient cloudClient = cluster.getSolrClient();
    List<MockSpan> allSpans = MockTracerConfigurator.TRACER.finishedSpans();

    cloudClient.add(COLLECTION, sdoc("id", "1"));
    List<MockSpan> finishedSpans = getRecentSpans(allSpans);
    finishedSpans.removeIf(x ->
        !x.tags().get("http.url").toString().endsWith("/update"));
    assertEquals(2, finishedSpans.size());
    assertOneSpanIsChildOfAnother(finishedSpans);

    cloudClient.add(COLLECTION, sdoc("id", "2"));
    finishedSpans = getRecentSpans(allSpans);
    finishedSpans.removeIf(x ->
        !x.tags().get("http.url").toString().endsWith("/update"));
    assertEquals(2, finishedSpans.size());
    assertOneSpanIsChildOfAnother(finishedSpans);

    cloudClient.add(COLLECTION, sdoc("id", "3"));
    cloudClient.add(COLLECTION, sdoc("id", "4"));
    cloudClient.commit(COLLECTION);

    getRecentSpans(allSpans);
    cloudClient.query(COLLECTION, new SolrQuery("*:*"));
    finishedSpans = getRecentSpans(allSpans);
    finishedSpans.removeIf(x ->
        !x.tags().get("http.url").toString().endsWith("/select"));
    // one from client to server, 2 for execute query, 2 for fetching documents
    assertEquals(5, finishedSpans.size());
    assertEquals(1, finishedSpans.stream().filter(s -> s.parentId() == 0).count());
    long parentId = finishedSpans.stream()
        .filter(s -> s.parentId() == 0)
        .collect(Collectors.toList())
        .get(0).context().spanId();
    for (MockSpan span: finishedSpans) {
      if (span.parentId() != 0 && parentId != span.parentId()) {
        fail("All spans must belong to single span, but:"+finishedSpans);
      }
    }
  }

  private void assertOneSpanIsChildOfAnother(List<MockSpan> finishedSpans) {
    MockSpan child = finishedSpans.get(0);
    MockSpan parent = finishedSpans.get(1);
    if (child.parentId() == 0) {
      MockSpan temp = parent;
      parent = child;
      child = temp;
    }

    assertEquals(child.parentId(), parent.context().spanId());
  }

  private List<MockSpan> getRecentSpans(List<MockSpan> allSpans) {
    List<MockSpan> result = new ArrayList<>(MockTracerConfigurator.TRACER.finishedSpans());
    result.removeAll(allSpans);
    allSpans.clear();
    allSpans.addAll(MockTracerConfigurator.TRACER.finishedSpans());
    return result;
  }
}
