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

import com.codahale.metrics.Gauge;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.util.stats.MetricUtils;
import org.junit.BeforeClass;
import org.junit.Test;

public class RequestHandlersTest extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }

  @Test
  public void testInitCount() {
    String registry = h.getCore().getCoreMetricManager().getRegistryName();
    SolrMetricManager manager = h.getCoreContainer().getMetricManager();
    @SuppressWarnings({"unchecked"})
    Gauge<Number> g = (Gauge<Number>)manager.registry(registry).getMetrics().get("QUERY./mock.initCount");
    assertEquals("Incorrect init count",
                 1, g.getValue().intValue());
  }

  @Test
  public void testImplicitRequestHandlers(){
    SolrCore core = h.getCore();
    assertNotNull(core.getRequestHandler( "/update/json"));
    assertNotNull(core.getRequestHandler( "/update/json/docs"));
    assertNotNull(core.getRequestHandler( "/update/csv"));
  }

  @Test
  public void testLazyLoading() {
    SolrCore core = h.getCore();
    PluginBag.PluginHolder<SolrRequestHandler> handler = core.getRequestHandlers().getRegistry().get("/lazy");
    assertFalse(handler.isLoaded());
    
    assertU(adoc("id", "42",
                 "name", "Zapp Brannigan"));
    assertU(adoc("id", "43",
                 "title", "Democratic Order of Planets"));
    assertU(adoc("id", "44",
                 "name", "The Zapper"));
    assertU(adoc("id", "45",
                 "title", "25 star General"));
    assertU(adoc("id", "46",
                 "subject", "Defeated the pacifists of the Gandhi nebula"));
    assertU(adoc("id", "47",
                 "text", "line up and fly directly at the enemy death cannons, clogging them with wreckage!"));
    assertU(commit());

    assertQ("lazy request handler returns all matches",
            req("q","id:[42 TO 47]"),
            "*[count(//doc)=6]");

        // But it should behave just like the 'defaults' request handler above
    assertQ("lazy handler returns fewer matches",
            req("q", "id:[42 TO 47]", "qt","/lazy"),
            "*[count(//doc)=4]"
            );

    assertQ("lazy handler includes highlighting",
            req("q", "name:Zapp OR title:General", "qt","/lazy"),
            "//lst[@name='highlighting']"
            );
  }

  @Test
  public void testPathNormalization()
  {
    SolrCore core = h.getCore();
    SolrRequestHandler h1 = core.getRequestHandler("/update" );
    assertNotNull( h1 );

    SolrRequestHandler h2 = core.getRequestHandler("/update/" );
    assertNotNull( h2 );
    
    assertEquals( h1, h2 ); // the same object
    
    assertNull( core.getRequestHandler("/update/asdgadsgas" ) ); // prefix
  }

  @Test
  public void testStatistics() {
    SolrCore core = h.getCore();
    SolrRequestHandler updateHandler = core.getRequestHandler("/update");
    SolrRequestHandler termHandler = core.getRequestHandler("/terms");

    assertU(adoc("id", "47",
        "text", "line up and fly directly at the enemy death cannons, clogging them with wreckage!"));
    assertU(commit());

    Map<String,Object> updateStats = MetricUtils.convertMetrics(updateHandler.getMetricRegistry(), updateHandler.getMetricNames());
    Map<String,Object> termStats = MetricUtils.convertMetrics(termHandler.getMetricRegistry(), termHandler.getMetricNames());

    Long updateTime = (Long) updateStats.get("UPDATE./update.totalTime");
    Long termTime = (Long) termStats.get("QUERY./terms.totalTime");

    assertFalse("RequestHandlers should not share statistics!", updateTime.equals(termTime));
  }
}
