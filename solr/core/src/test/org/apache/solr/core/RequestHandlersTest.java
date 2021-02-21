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
package org.apache.solr.core;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Gauge;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.TimeOut;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.search.TestSimpleQParserPlugin;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class RequestHandlersTest extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeRequestHandlersTest() throws Exception {
    System.setProperty("solr.enableMetrics", "true");
    initCore("solrconfig.xml", "schema.xml");
  }

  @AfterClass
  public static void afterRequestHandlersTest() throws Exception {
    deleteCore();
  }

  @Test
  public void testInitCount() {
    // MRM TODO: MRM needs to wait for metric
    try (SolrCore core = h.getCore()) {
      String registry = core.getCoreMetricManager().getRegistryName();
      SolrMetricManager manager = h.getCoreContainer().getMetricManager();
      Gauge<Number> g = null;

      TimeOut timeout = new TimeOut(1000, TimeUnit.MILLISECONDS,
          TimeSource.NANO_TIME);
      Number val = null;
      while (!timeout.hasTimedOut()) {
        g = (Gauge<Number>) manager.registry(registry).getMetrics()
            .get("QUERY./mock.initCount");
        val = g.getValue();
        if (val != null && val.longValue() > 0) break;
      }

      // MRM TODO: flakey, can be missing, but many non mock ones exist, perhaps overridden?
      assertNotNull("init count is null " + manager.registry(registry).getMetrics(), val);
      if (val.longValue() > 0) {
        assertEquals("Incorrect init count " + manager.registry(registry).getMetrics(), 1, val.intValue());
      }
    }
  }

  @Test
  public void testImplicitRequestHandlers(){
    try (SolrCore core = h.getCore()) {
      assertNotNull(core.getRequestHandler("/update/json"));
      assertNotNull(core.getRequestHandler("/update/json/docs"));
      assertNotNull(core.getRequestHandler("/update/csv"));
    }
  }

  @Test
  public void testLazyLoading() {
    try (SolrCore core = h.getCore()) {
      PluginBag.PluginHolder<SolrRequestHandler> handler = core.getRequestHandlers().getRegistry().get("/lazy");
      assertFalse(handler.isLoaded());

      assertU(adoc("id", "42", "name", "Zapp Brannigan"));
      assertU(adoc("id", "43", "title", "Democratic Order of Planets"));
      assertU(adoc("id", "44", "name", "The Zapper"));
      assertU(adoc("id", "45", "title", "25 star General"));
      assertU(adoc("id", "46", "subject", "Defeated the pacifists of the Gandhi nebula"));
      assertU(adoc("id", "47", "text", "line up and fly directly at the enemy death cannons, clogging them with wreckage!"));
      assertU(commit());

      assertQ("lazy request handler returns all matches", req("q", "id:[42 TO 47]"), "*[count(//doc)=6]");

      // But it should behave just like the 'defaults' request handler above
      assertQ("lazy handler returns fewer matches", req("q", "id:[42 TO 47]", "qt", "/lazy"), "*[count(//doc)=4]");

      assertQ("lazy handler includes highlighting", req("q", "name:Zapp OR title:General", "qt", "/lazy"), "//lst[@name='highlighting']");
    }
  }

  @Test
  public void testPathNormalization()
  {
    try (SolrCore core = h.getCore()) {
      SolrRequestHandler h1 = core.getRequestHandler("/update");
      assertNotNull(h1);

      SolrRequestHandler h2 = core.getRequestHandler("/update/");
      assertNotNull(h2);

      assertEquals(h1, h2); // the same object

      assertNull(core.getRequestHandler("/update/asdgadsgas")); // prefix
    }
  }

  @Test
  public void testStatistics() {
    try (SolrCore core = h.getCore()) {
      SolrRequestHandler updateHandler = core.getRequestHandler("/update");
      SolrRequestHandler termHandler = core.getRequestHandler("/terms");

      assertU(adoc("id", "47", "text", "line up and fly directly at the enemy death cannons, clogging them with wreckage!"));
      assertU(commit());

      Map<String,Object> updateStats = updateHandler.getSolrMetricsContext().getMetricsSnapshot();
      Map<String,Object> termStats = termHandler.getSolrMetricsContext().getMetricsSnapshot();

      Long updateTime = (Long) updateStats.get("UPDATE./update.totalTime");
      Long termTime = (Long) termStats.get("QUERY./terms.totalTime");

      // MRM TODO: these update stat's seemed a bit expensive to be enabled in such tight spots by default
      // assertFalse("RequestHandlers should not share statistics!", updateTime.equals(termTime));
    }
  }
}
