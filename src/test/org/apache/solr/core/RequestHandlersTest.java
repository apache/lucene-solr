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

import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.request.StandardRequestHandler;
import org.apache.solr.util.AbstractSolrTestCase;

public class RequestHandlersTest extends AbstractSolrTestCase {

  public String getSchemaFile() { return "schema.xml"; }
  public String getSolrConfigFile() { return "solrconfig.xml"; }

  
  public void testLazyLoading() {
    SolrCore core = SolrCore.getSolrCore();
    SolrRequestHandler handler = core.getRequestHandler( "lazy" );
    assertFalse( handler instanceof StandardRequestHandler ); 
    
    // But it should behave just like the 'defaults' request handler above
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
            req("id:[42 TO 47]"),
            "*[count(//doc)=6]"
            );

    assertQ("lazy handler returns fewer matches",
            req("q", "id:[42 TO 47]",   "qt","defaults"),
            "*[count(//doc)=4]"
            );

    assertQ("lazy handler includes highlighting",
            req("q", "name:Zapp OR title:General",   "qt","defaults"),
            "//lst[@name='highlighting']"
            );
  }
}
