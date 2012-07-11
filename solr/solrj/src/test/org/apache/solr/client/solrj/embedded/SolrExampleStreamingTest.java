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

package org.apache.solr.client.solrj.embedded;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.SolrExampleTests;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrServer;
import org.apache.solr.client.solrj.impl.XMLResponseParser;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.util.ExternalPaths;
import org.junit.BeforeClass;


/**
 * 
 *
 * @since solr 1.3
 */
@Slow
public class SolrExampleStreamingTest extends SolrExampleTests {
  @BeforeClass
  public static void beforeTest() throws Exception {
    createJetty(ExternalPaths.EXAMPLE_HOME, null, null);
  }

  @Override
  public SolrServer createNewSolrServer()
  {
    try {
      // setup the server...
      String url = "http://localhost:"+port+context;       // smaller queue size hits locks more often
      ConcurrentUpdateSolrServer s = new ConcurrentUpdateSolrServer( url, 2, 5 ) {
        
        public Throwable lastError = null;
        @Override
        public void handleError(Throwable ex) {
          lastError = ex;
        }
      };

      s.setParser(new XMLResponseParser());
      s.setRequestWriter(new RequestWriter());
      return s;
    }
    
    catch( Exception ex ) {
      throw new RuntimeException( ex );
    }
  }
}
