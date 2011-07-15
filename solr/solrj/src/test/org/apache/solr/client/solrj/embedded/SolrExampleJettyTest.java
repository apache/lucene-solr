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

package org.apache.solr.client.solrj.embedded;

import org.apache.solr.client.solrj.SolrExampleTests;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.util.ExternalPaths;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * TODO? perhaps use:
 *  http://docs.codehaus.org/display/JETTY/ServletTester
 * rather then open a real connection?
 * 
 */
public class SolrExampleJettyTest extends SolrExampleTests {

  @BeforeClass
  public static void beforeTest() throws Exception {
    createJetty(ExternalPaths.EXAMPLE_HOME, null, null);
  }

  @Test
  public void testBadSetup()
  {
    try {
      // setup the server...
      String url = "http://localhost/?core=xxx";
      CommonsHttpSolrServer s = new CommonsHttpSolrServer( url );
      Assert.fail( "CommonsHttpSolrServer should not allow a path with a parameter: "+s.getBaseURL() );
    }
    catch( Exception ex ) {
      // expected
    }
  }
}
