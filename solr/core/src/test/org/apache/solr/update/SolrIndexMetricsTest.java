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
package org.apache.solr.update;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.util.TimeOut;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.management.InstanceNotFoundException;

/**
 * Test proper registration and collection of index and directory metrics.
 */
public class SolrIndexMetricsTest extends SolrTestCaseJ4 {

  @Before
  public void setUp() throws Exception {
    if (TEST_NIGHTLY) {
      System.setProperty("solr.tests.maxBufferedDocs", "20");
    } else {
      System.setProperty("solr.tests.maxBufferedDocs", "10");
    }
    super.setUp();
  }

  @After
  public void tearDown() throws Exception {
    deleteCore();
    super.tearDown();
  }

  private void addDocs() throws Exception {
    try (SolrQueryRequest req = lrf.makeRequest()) {
      UpdateHandler uh = req.getCore().getUpdateHandler();
      AddUpdateCommand add = new AddUpdateCommand(req);
      for (int i = 0; i < (TEST_NIGHTLY ? 1000 : 100); i++) {
        add.clear();
        add.setReq(req);
        add.solrDoc = new SolrInputDocument();
        add.solrDoc.addField("id", "" + i);
        add.solrDoc.addField("foo_s", "foo-" + i);
        uh.addDoc(add);
      }
      uh.commit(new CommitUpdateCommand(req, false));
      // make sure all merges are finished
      h.reload();
    }
  }

  @Test
  public void testIndexMetricsNoDetails() throws Exception {
    System.setProperty("solr.tests.metrics.merge", "true");
    System.setProperty("solr.tests.metrics.mergeDetails", "false");
    try {
      initCore("solrconfig-indexmetrics.xml", "schema.xml");

      addDocs();

      SolrCore core = h.getCore();

      MetricRegistry registry = null;
      Map<String, Metric> metrics = null;

      TimeOut timeout = new TimeOut(1000, TimeUnit.MILLISECONDS, TimeSource.NANO_TIME);
      while (!timeout.hasTimedOut()) {
        registry = h.getCoreContainer().getMetricManager().registry(core.getCoreMetricManager().getRegistryName());
        assertNotNull(registry);
        metrics = registry.getMetrics();
        if (metrics.entrySet().stream().filter(e -> e.getKey().startsWith("INDEX")).count() > 11) break;
      }

      core.close();

      if (timeout.hasTimedOut()) {
        fail("Timeout waiting for correct metrics count");
      }

      // check basic index meters
      Timer timer = (Timer) metrics.get("INDEX.merge.minor");
      assertTrue("minorMerge: " + timer.getCount(), timer.getCount() >= 3);
      timer = (Timer) metrics.get("INDEX.merge.major");
      assertEquals("majorMerge: " + timer.getCount(), 0, timer.getCount());
      // check detailed meters
      assertNull((Meter) metrics.get("INDEX.merge.major.docs"));
      Meter meter = (Meter) metrics.get("INDEX.flush");
      assertTrue("flush: " + meter.getCount(), meter.getCount() > 10);
    } finally {
      deleteCore();
    }
  }

  @Test
  public void testIndexNoMetrics() throws Exception {
    System.setProperty("solr.tests.metrics.merge", "false");
    System.setProperty("solr.tests.metrics.mergeDetails", "false");
    initCore("solrconfig-indexmetrics.xml", "schema.xml");

    addDocs();

    SolrCore core = h.getCore();
    MetricRegistry registry = null;

    TimeOut timeout = new TimeOut(1000, TimeUnit.MILLISECONDS, TimeSource.NANO_TIME);
    while (!timeout.hasTimedOut()) {
      registry = h.getCoreContainer().getMetricManager().registry(core.getCoreMetricManager().getRegistryName());
      Map<String, Metric> metrics = registry.getMetrics();
      if (metrics.entrySet().stream().filter(e -> e.getKey().startsWith("INDEX")).count() > 1) break;
    }

    if (timeout.hasTimedOut()) {
      fail("Timeout waiting for correct metrics count");
    }

    // INDEX.size, INDEX.sizeInBytes
    core.close();
  }

  @Test
  public void testIndexMetricsWithDetails() throws Exception {
    System.setProperty("solr.tests.metrics.merge", "false"); // test mergeDetails override too
    System.setProperty("solr.tests.metrics.mergeDetails", "true");
    initCore("solrconfig-indexmetrics.xml", "schema.xml");

    addDocs();
    SolrCore core = h.getCore();
    MetricRegistry registry = h.getCoreContainer().getMetricManager().registry(core.getCoreMetricManager().getRegistryName());
    core.close();
    assertNotNull(registry);

    Map<String, Metric> metrics = registry.getMetrics();

    assertTrue(metrics.entrySet().stream().filter(e -> e.getKey().startsWith("INDEX")).count() >= 12);

    // check basic index meters
    Timer timer = (Timer)metrics.get("INDEX.merge.minor");
    assertTrue("minorMerge: " + timer.getCount(), timer.getCount() >= 3);
    timer = (Timer)metrics.get("INDEX.merge.major");
    assertEquals("majorMerge: " + timer.getCount(), 0, timer.getCount());
    // check detailed meters
    Meter meter = (Meter)metrics.get("INDEX.merge.major.docs");
    assertEquals("majorMergeDocs: " + meter.getCount(), 0, meter.getCount());

    meter = (Meter)metrics.get("INDEX.flush");
    assertTrue("flush: " + meter.getCount(), meter.getCount() > 10);
  }
}
