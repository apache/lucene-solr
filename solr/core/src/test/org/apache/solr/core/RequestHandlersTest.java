/**
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

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.handler.StandardRequestHandler;
import org.apache.solr.request.SolrRequestHandler;
import org.junit.BeforeClass;
import org.junit.Test;

public class RequestHandlersTest extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }

  @Test
  public void testInitCount() {
    SolrCore core = h.getCore();
    SolrRequestHandler handler = core.getRequestHandler( "mock" );
    assertEquals("Incorrect init count",
                 1, handler.getStatistics().get("initCount"));
  }

  @Test
  public void testLazyLoading() {
    SolrCore core = h.getCore();
    SolrRequestHandler handler = core.getRequestHandler( "lazy" );
    assertFalse( handler instanceof StandardRequestHandler ); 
    
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
            req("q", "id:[42 TO 47]", "qt","lazy"),
            "*[count(//doc)=4]"
            );

    assertQ("lazy handler includes highlighting",
            req("q", "name:Zapp OR title:General", "qt","lazy"),
            "//lst[@name='highlighting']"
            );
  }

  @Test
  public void testPathNormalization()
  {
    SolrCore core = h.getCore();
    SolrRequestHandler h1 = core.getRequestHandler("/update/csv" );
    assertNotNull( h1 );

    SolrRequestHandler h2 = core.getRequestHandler("/update/csv/" );
    assertNotNull( h2 );
    
    assertEquals( h1, h2 ); // the same object
    
    assertNull( core.getRequestHandler("/update/csv/asdgadsgas" ) ); // prefix
  }
}
