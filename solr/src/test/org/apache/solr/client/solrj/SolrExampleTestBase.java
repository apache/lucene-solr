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

package org.apache.solr.client.solrj;


import org.apache.solr.util.AbstractSolrTestCase;

/**
 * This should include tests against the example solr config
 * 
 * This lets us try various SolrServer implementations with the same tests.
 * 
 *
 * @since solr 1.3
 */
abstract public class SolrExampleTestBase extends AbstractSolrTestCase 
{
  @Override
  public String getSolrHome() { return "../../../example/solr/"; }
  
  @Override public String getSchemaFile()     { return getSolrHome()+"conf/schema.xml";     }
  @Override public String getSolrConfigFile() { return getSolrHome()+"conf/solrconfig.xml"; }
 
  @Override
  public void setUp() throws Exception
  {
    ignoreException("maxWarmingSearchers");
    super.setUp();
    
    // this sets the property for jetty starting SolrDispatchFilter
    System.setProperty( "solr.solr.home", this.getSolrHome() ); 
    System.setProperty( "solr.data.dir", this.dataDir.getCanonicalPath() ); 
  }
  
  /**
   * Subclasses need to initialize the server impl
   */
  protected abstract SolrServer getSolrServer();
  
  /**
   * Create a new solr server
   */
  protected abstract SolrServer createNewSolrServer();
}
