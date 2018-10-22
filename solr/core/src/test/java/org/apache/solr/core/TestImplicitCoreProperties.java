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

import java.util.Properties;

import org.apache.solr.SolrTestCaseJ4;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestImplicitCoreProperties extends SolrTestCaseJ4 {

  private static CoreContainer cc;

  @BeforeClass
  public static void setupContainer() {
    cc = createCoreContainer("collection1", "data", "solrconfig-implicitproperties.xml", "schema.xml");
  }

  @AfterClass
  public static void teardownContainer() {
    if (cc != null) {
      cc.shutdown();
    }
    cc = null;
  }

  @Test
  public void testImplicitPropertiesAreSubstitutedInSolrConfig() {
    assertQ(req("q", "*:*")
        , "//str[@name='dummy1'][.='collection1']"
        , "//str[@name='dummy2'][.='data']"
        , "//str[@name='dummy3'][.='solrconfig-implicitproperties.xml']"
        , "//str[@name='dummy4'][.='schema.xml']"
        , "//str[@name='dummy5'][.='false']"
    );
  }

  // SOLR-5279
  @Test
  public void testPropertiesArePersistedAcrossReload() {
    cc.reload("collection1");
    assertQ(req("q", "*:*")
        , "//str[@name='dummy1'][.='collection1']"
        , "//str[@name='dummy2'][.='data']"
        , "//str[@name='dummy3'][.='solrconfig-implicitproperties.xml']"
        , "//str[@name='dummy4'][.='schema.xml']"
        , "//str[@name='dummy5'][.='false']"
    );
  }

  // SOLR-8712
  @Test
  public void testDefaultProperties() {
    Properties props = cc.getCoreDescriptor("collection1").getSubstitutableProperties();
    assertEquals("collection1", props.getProperty("solr.core.name"));
    assertTrue("solr.core.instanceDir not set correctly",
        props.getProperty("solr.core.instanceDir").contains("collection1"));
  }

}
