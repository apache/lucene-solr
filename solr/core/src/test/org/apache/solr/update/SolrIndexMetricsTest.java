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

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test proper registration and collection of index and directory metrics.
 */
public class SolrIndexMetricsTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty("solr.tests.mergeDetails", "true");
    System.setProperty("solr.tests.directoryDetails", "true");
    initCore("solrconfig-indexmetrics.xml", "schema.xml");
  }

  @Test
  public void testIndexMetrics() throws Exception {
    SolrQueryRequest req = lrf.makeRequest();
    UpdateHandler uh = req.getCore().getUpdateHandler();
    AddUpdateCommand add = new AddUpdateCommand(req);
    for (int i = 0; i < 1000; i++) {
      add.clear();
      add.solrDoc = new SolrInputDocument();
      add.solrDoc.addField("id", "" + i);
      add.solrDoc.addField("foo_s", "foo-" + i);
      uh.addDoc(add);
    }
    uh.commit(new CommitUpdateCommand(req, false));
    MetricRegistry registry = h.getCoreContainer().getMetricManager().registry(h.getCore().getCoreMetricManager().getRegistryName());
    assertNotNull(registry);
    // make sure all merges are finished
    h.reload();

    Map<String, Metric> metrics = registry.getMetrics();

    assertTrue(metrics.entrySet().stream().filter(e -> e.getKey().startsWith("INDEX")).count() >= 12);
    // this is variable, depending on the codec and the number of created files
    assertTrue(metrics.entrySet().stream().filter(e -> e.getKey().startsWith("DIRECTORY")).count() > 50);

    // check basic index meters
    Timer timer = (Timer)metrics.get("INDEX.merge.minor");
    assertTrue("minorMerge: " + timer.getCount(), timer.getCount() >= 3);
    timer = (Timer)metrics.get("INDEX.merge.major");
    assertEquals("majorMerge: " + timer.getCount(), 0, timer.getCount());
    Meter meter = (Meter)metrics.get("INDEX.merge.major.docs");
    assertEquals("majorMergeDocs: " + meter.getCount(), 0, meter.getCount());
    meter = (Meter)metrics.get("INDEX.flush");
    assertTrue("flush: " + meter.getCount(), meter.getCount() > 10);

    // check basic directory meters
    meter = (Meter)metrics.get("DIRECTORY.total.reads");
    assertTrue("totalReads", meter.getCount() > 0);
    meter = (Meter)metrics.get("DIRECTORY.total.writes");
    assertTrue("totalWrites", meter.getCount() > 0);
    Histogram histogram = (Histogram)metrics.get("DIRECTORY.total.readSizes");
    assertTrue("readSizes", histogram.getCount() > 0);
    histogram = (Histogram)metrics.get("DIRECTORY.total.writeSizes");
    assertTrue("writeSizes", histogram.getCount() > 0);
    // check detailed meters
    meter = (Meter)metrics.get("DIRECTORY.segments.writes");
    assertTrue("segmentsWrites", meter.getCount() > 0);
    histogram = (Histogram)metrics.get("DIRECTORY.segments.writeSizes");
    assertTrue("segmentsWriteSizes", histogram.getCount() > 0);

  }
}
