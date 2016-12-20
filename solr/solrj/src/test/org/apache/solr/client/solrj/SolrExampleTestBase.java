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
package org.apache.solr.client.solrj;

import org.apache.solr.util.AbstractSolrTestCase;
import org.junit.BeforeClass;

/**
 * This should include tests against the example solr config
 * 
 * This lets us try various SolrServer implementations with the same tests.
 * 
 */
abstract public class SolrExampleTestBase extends AbstractSolrTestCase {
  @Override
  public String getSolrHome() {
    return "../../../example/solr/";
  }
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    
  }
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    
    // this sets the property for jetty starting SolrDispatchFilter
    System.setProperty("solr.solr.home", this.getSolrHome());
    System.setProperty("solr.data.dir", this.initCoreDataDir.getCanonicalPath());
  }
  
  @Override
  public void tearDown() throws Exception {
    System.clearProperty("solr.solr.home");
    System.clearProperty("solr.data.dir");
    super.tearDown();
  }
  
  /**
   * Subclasses need to initialize the server impl
   */
  protected abstract SolrClient getSolrClient();
  
  /**
   * Create a new solr server
   */
  protected abstract SolrClient createNewSolrClient();
}
