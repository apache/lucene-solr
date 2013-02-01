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

package org.apache.solr.update;

import java.util.HashMap;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.UpdateRequestHandler;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.AbstractSolrTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

@Slow
public class HardAutoCommitTest extends AbstractSolrTestCase {

  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty("solr.commitwithin.softcommit", "false");
    initCore("solrconfig.xml", "schema.xml");
  }
  
  @AfterClass
  public static void afterClass() {
    System.clearProperty("solr.commitwithin.softcommit");
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    clearIndex();
    // reload the core to clear stats
    h.getCoreContainer().reload(h.getCore().getName());
  }


  public void testCommitWithin() throws Exception {
    SolrCore core = h.getCore();
    
    NewSearcherListener trigger = new NewSearcherListener();    
    core.registerNewSearcherListener(trigger);
    DirectUpdateHandler2 updater = (DirectUpdateHandler2) core.getUpdateHandler();
    CommitTracker tracker = updater.commitTracker;
    tracker.setTimeUpperBound(0);
    tracker.setDocsUpperBound(-1);
    
    UpdateRequestHandler handler = new UpdateRequestHandler();
    handler.init( null );
    
    MapSolrParams params = new MapSolrParams( new HashMap<String, String>() );
    
    // Add a single document with commitWithin == 2 second
    SolrQueryResponse rsp = new SolrQueryResponse();
    SolrQueryRequestBase req = new SolrQueryRequestBase( core, params ) {};
    req.setContentStreams( AutoCommitTest.toContentStreams(
      adoc(2000, "id", "529", "field_t", "what's inside?", "subject", "info"), null ) );
    trigger.reset();
    handler.handleRequest( req, rsp );

    // Check it isn't in the index
    assertQ("shouldn't find any", req("id:529") ,"//result[@numFound=0]" );
    
    // Wait longer than the commitWithin time
    assertTrue("commitWithin failed to commit", trigger.waitForNewSearcher(30000));

    // Add one document without commitWithin
    req.setContentStreams( AutoCommitTest.toContentStreams(
        adoc("id", "530", "field_t", "what's inside?", "subject", "info"), null ) );
      trigger.reset();
      handler.handleRequest( req, rsp );
      
    // Check it isn't in the index
    assertQ("shouldn't find any", req("id:530") ,"//result[@numFound=0]" );
    
    // Delete one document with commitWithin
    req.setContentStreams( AutoCommitTest.toContentStreams(
      delI("529", "commitWithin", "1000"), null ) );
    trigger.reset();
    handler.handleRequest( req, rsp );
      
    // Now make sure we can find it
    assertQ("should find one", req("id:529") ,"//result[@numFound=1]" );
    
    // Wait for the commit to happen
    assertTrue("commitWithin failed to commit", trigger.waitForNewSearcher(30000));
    
    // Now we shouldn't find it
    assertQ("should find none", req("id:529") ,"//result[@numFound=0]" );
    // ... but we should find the new one
    assertQ("should find one", req("id:530") ,"//result[@numFound=1]" );
    
    trigger.reset();
    
    // now make the call 10 times really fast and make sure it 
    // only commits once
    req.setContentStreams( AutoCommitTest.toContentStreams(
        adoc(2000, "id", "500" ), null ) );
    for( int i=0;i<10; i++ ) {
      handler.handleRequest( req, rsp );
    }
    assertQ("should not be there yet", req("id:500") ,"//result[@numFound=0]" );
    
    // the same for the delete
    req.setContentStreams( AutoCommitTest.toContentStreams(
        delI("530", "commitWithin", "1000"), null ) );
    for( int i=0;i<10; i++ ) {
      handler.handleRequest( req, rsp );
    }
    assertQ("should be there", req("id:530") ,"//result[@numFound=1]" );
    
    assertTrue("commitWithin failed to commit", trigger.waitForNewSearcher(30000));
    assertQ("should be there", req("id:500") ,"//result[@numFound=1]" );
    assertQ("should not be there", req("id:530") ,"//result[@numFound=0]" );
    
    assertEquals(3, tracker.getCommitCount());
  }

}
