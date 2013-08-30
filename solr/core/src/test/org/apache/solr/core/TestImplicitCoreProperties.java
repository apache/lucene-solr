package org.apache.solr.core;

import org.apache.solr.SolrTestCaseJ4;
import org.junit.Test;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
public class TestImplicitCoreProperties extends SolrTestCaseJ4 {

  public static final String SOLRXML =
      "<solr><cores><core name=\"collection1\" instanceDir=\"collection1\" config=\"solrconfig-implicitproperties.xml\"/></cores></solr>";

  @Test
  public void testImplicitPropertiesAreSubstitutedInSolrConfig() {

    CoreContainer cc = createCoreContainer(TEST_HOME(), SOLRXML);
    try {
      cc.load();
      assertQ(req("q", "*:*"), "//str[@name='dummyParam'][.='collection1']");
    }
    finally {
      cc.shutdown();
    }

  }

}
