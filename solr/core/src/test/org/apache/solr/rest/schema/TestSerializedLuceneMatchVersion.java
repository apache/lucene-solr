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
package org.apache.solr.rest.schema;
import org.apache.solr.util.RestTestBase;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.SortedMap;
import java.util.TreeMap;


public class TestSerializedLuceneMatchVersion extends RestTestBase {

  @BeforeClass
  public static void init() throws Exception {
    final SortedMap<ServletHolder,String> extraServlets = new TreeMap<>();

    createJettyAndHarness(TEST_HOME(), "solrconfig-minimal.xml", "schema-rest-lucene-match-version.xml",
                          "/solr", true, extraServlets);
  }

  @Test
  public void testExplicitLuceneMatchVersions() throws Exception {
    assertQ("/schema/fieldtypes/explicitLuceneMatchVersions?indent=on&wt=xml&showDefaults=true",
            "count(/response/lst[@name='fieldType']) = 1",
        
            "//lst[str[@name='class'][.='org.apache.solr.analysis.MockCharFilterFactory']]"
           +"     [str[@name='luceneMatchVersion'][.='4.0.0']]",
        
            "//lst[str[@name='class'][.='org.apache.solr.analysis.MockTokenizerFactory']]"
           +"     [str[@name='luceneMatchVersion'][.='4.0.0']]",
        
            "//lst[str[@name='class'][.='org.apache.solr.analysis.MockTokenFilterFactory']]"
           +"     [str[@name='luceneMatchVersion'][.='4.0.0']]");
  }

  @Test
  public void testNoLuceneMatchVersions() throws Exception {
    assertQ("/schema/fieldtypes/noLuceneMatchVersions?indent=on&wt=xml&showDefaults=true",
            "count(/response/lst[@name='fieldType']) = 1",

            "//lst[str[@name='class'][.='org.apache.solr.analysis.MockCharFilterFactory']]"
           +"     [not(./str[@name='luceneMatchVersion'])]",

            "//lst[str[@name='class'][.='org.apache.solr.analysis.MockTokenizerFactory']]"
           +"     [not(./str[@name='luceneMatchVersion'])]",
        
            "//lst[str[@name='class'][.='org.apache.solr.analysis.MockTokenFilterFactory']]"
           +"     [not(./str[@name='luceneMatchVersion'])]");
  }
  
}
