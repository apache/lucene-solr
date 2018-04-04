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
package org.apache.solr;

import org.apache.solr.common.params.CommonParams;
import org.apache.solr.request.*;
import org.apache.solr.util.*;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * This is an example of how to write a JUnit tests for Solr using the
 * SolrTestCaseJ4
 */
public class SampleTest extends SolrTestCaseJ4 {

  /**
   * All subclasses of SolrTestCaseJ4 should initialize the core.
   *
   * <p>
   * Note that different tests can use different schemas/configs by referring
   * to any crazy path they want (as long as it works).
   * </p>
   */
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solr/crazy-path-to-config.xml","solr/crazy-path-to-schema.xml");
  }
    
  /**
   * Demonstration of some of the simple ways to use the base class
   */
  @Test
  public void testSimple() {
    lrf.args.put(CommonParams.VERSION,"2.2");
    assertU("Simple assertion that adding a document works",
            adoc("id",  "4055",
                 "subject", "Hoss the Hoss man Hostetter"));

    /* alternate syntax, no label */
    assertU(adoc("id",  "4056",
                 "subject", "Some Other Guy"));

    assertU(commit());
    assertU(optimize());

    assertQ("couldn't find subject hoss",
            req("subject:Hoss")
            ,"//result[@numFound=1]"
            ,"//str[@name='id'][.='4055']"
            );
  }

  /**
   * Demonstration of some of the more complex ways to use the base class
   */
  @Test
  public void testAdvanced() throws Exception {
    lrf.args.put(CommonParams.VERSION,"2.2");        
    assertU("less common case, a complex addition with options",
            add(doc("id", "4059",
                    "subject", "Who Me?"),
                "overwrite", "false"));

    assertU("or just make the raw XML yourself",
            "<add overwrite=\"false\">" +
            doc("id", "4059",
                "subject", "Who Me Again?") + "</add>");

    /* or really make the xml yourself */
    assertU("<add><doc><field name=\"id\">4055</field>"
            +"<field name=\"subject\">Hoss the Hoss man Hostetter</field>"
            +"</doc></add>");
        
    assertU("<commit/>");
    assertU("<optimize/>");
        
    /* access the default LocalRequestFactory directly to make a request */
    SolrQueryRequest req = lrf.makeRequest( "subject:Hoss" );
    assertQ("couldn't find subject hoss",
            req
            ,"//result[@numFound=1]"
            ,"//str[@name='id'][.='4055']"
            );

    /* make your own LocalRequestFactory to build a request
     *
     * Note: the qt proves we are using our custom config...
     */
    TestHarness.LocalRequestFactory l = h.getRequestFactory
      ("/crazy_custom_qt",100,200,CommonParams.VERSION,"2.2");
    assertQ("how did i find Mack Daddy? ",
            l.makeRequest( "Mack Daddy" )
            ,"//result[@numFound=0]"
            );

    /* you can access the harness directly as well*/
    assertNull("how did i find Mack Daddy? ",
               h.validateQuery(l.makeRequest( "Mack Daddy" )
                               ,"//result[@numFound=0]"
                               ));
        
  }
}


