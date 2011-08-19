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

package org.apache.solr.update;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrEventListener;
import org.apache.solr.handler.XmlUpdateRequestHandler;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.AbstractSolrTestCase;

class NewSearcherListener implements SolrEventListener {
  private volatile boolean triggered = false;
  
  @Override
  public void init(NamedList args) {}
  
  @Override
  public void newSearcher(SolrIndexSearcher newSearcher,
      SolrIndexSearcher currentSearcher) {
    triggered = true;
  }
  
  @Override
  public void postCommit() {}
  
  @Override
  public void postSoftCommit() {}
  
  public void reset() {
    triggered = false;
  }
  
  boolean waitForNewSearcher(int timeout) {
    long timeoutTime = System.currentTimeMillis() + timeout;
    while (System.currentTimeMillis() < timeoutTime) {
      if (triggered) {
        return true;
      }
      
      try {
        Thread.sleep(250);
      } catch (InterruptedException e) {}
      
    }
    return false;
  }
}

public class AutoCommitTest extends AbstractSolrTestCase {

  @Override
  public String getSchemaFile() { return "schema.xml"; }
  @Override
  public String getSolrConfigFile() { return "solrconfig.xml"; }

  /**
   * Take a string and make it an iterable ContentStream
   * 
   * This should be moved to a helper class. (it is useful for the client too!)
   */
  public static Collection<ContentStream> toContentStreams( final String str, final String contentType )
  {
    ArrayList<ContentStream> streams = new ArrayList<ContentStream>();
    ContentStreamBase stream = new ContentStreamBase.StringStream( str );
    stream.setContentType( contentType );
    streams.add( stream );
    return streams;
  }

  public void testMaxDocs() throws Exception {

    SolrCore core = h.getCore();
    NewSearcherListener trigger = new NewSearcherListener();

    DirectUpdateHandler2 updateHandler = (DirectUpdateHandler2)core.getUpdateHandler();
    CommitTracker tracker = updateHandler.commitTracker;
    tracker.setTimeUpperBound(-1);
    tracker.setDocsUpperBound(14);
    core.registerNewSearcherListener(trigger);
  
    
    XmlUpdateRequestHandler handler = new XmlUpdateRequestHandler();
    handler.init( null );
    
    MapSolrParams params = new MapSolrParams( new HashMap<String, String>() );
    
    // Add documents
    SolrQueryResponse rsp = new SolrQueryResponse();
    SolrQueryRequestBase req = new SolrQueryRequestBase( core, params ) {};
    for( int i=0; i<14; i++ ) {
      req.setContentStreams( toContentStreams(
        adoc("id", Integer.toString(i), "subject", "info" ), null ) );
      handler.handleRequest( req, rsp );
    }
    // It should not be there right away
    assertQ("shouldn't find any", req("id:1") ,"//result[@numFound=0]" );
    assertEquals( 0, tracker.getCommitCount());

    req.setContentStreams( toContentStreams(
        adoc("id", "14", "subject", "info" ), null ) );
    handler.handleRequest( req, rsp );

    assertTrue(trigger.waitForNewSearcher(10000));

    req.setContentStreams( toContentStreams(
        adoc("id", "15", "subject", "info" ), null ) );
    handler.handleRequest( req, rsp );
      
    // Now make sure we can find it
    assertQ("should find one", req("id:14") ,"//result[@numFound=1]" );
    assertEquals( 1, tracker.getCommitCount());
    // But not the one added afterward
    assertQ("should not find one", req("id:15") ,"//result[@numFound=0]" );
    assertEquals( 1, tracker.getCommitCount());
    
  }

  public void testMaxTime() throws Exception {
    SolrCore core = h.getCore();
    NewSearcherListener trigger = new NewSearcherListener();    
    core.registerNewSearcherListener(trigger);
    DirectUpdateHandler2 updater = (DirectUpdateHandler2) core.getUpdateHandler();
    CommitTracker tracker = updater.commitTracker;
    // too low of a number can cause a slow host to commit before the test code checks that it
    // isn't there... causing a failure at "shouldn't find any"
    tracker.setTimeUpperBound(1000);
    tracker.setDocsUpperBound(-1);
    // updater.commitCallbacks.add(trigger);
    
    XmlUpdateRequestHandler handler = new XmlUpdateRequestHandler();
    handler.init( null );
    
    MapSolrParams params = new MapSolrParams( new HashMap<String, String>() );
    
    // Add a single document
    SolrQueryResponse rsp = new SolrQueryResponse();
    SolrQueryRequestBase req = new SolrQueryRequestBase( core, params ) {};
    req.setContentStreams( toContentStreams(
      adoc("id", "529", "field_t", "what's inside?", "subject", "info"), null ) );
    trigger.reset();
    handler.handleRequest( req, rsp );

    // Check it it is in the index
    assertQ("shouldn't find any", req("id:529") ,"//result[@numFound=0]" );

    // Wait longer than the autocommit time
    assertTrue(trigger.waitForNewSearcher(30000));
    trigger.reset();
    req.setContentStreams( toContentStreams(
      adoc("id", "530", "field_t", "what's inside?", "subject", "info"), null ) );
    handler.handleRequest( req, rsp );
      
    // Now make sure we can find it
    assertQ("should find one", req("id:529") ,"//result[@numFound=1]" );
    // But not this one
    assertQ("should find none", req("id:530") ,"//result[@numFound=0]" );
    
    // Delete the document
    assertU( delI("529") );
    assertQ("deleted, but should still be there", req("id:529") ,"//result[@numFound=1]" );
    // Wait longer than the autocommit time
    assertTrue(trigger.waitForNewSearcher(30000));
    trigger.reset();
    req.setContentStreams( toContentStreams(
      adoc("id", "550", "field_t", "what's inside?", "subject", "info"), null ) );
    handler.handleRequest( req, rsp );
    assertEquals( 2, tracker.getCommitCount() );
    assertQ("deleted and time has passed", req("id:529") ,"//result[@numFound=0]" );
    
    // now make the call 10 times really fast and make sure it 
    // only commits once
    req.setContentStreams( toContentStreams(
        adoc("id", "500" ), null ) );
    for( int i=0;i<10; i++ ) {
      handler.handleRequest( req, rsp );
    }
    assertQ("should not be there yet", req("id:500") ,"//result[@numFound=0]" );
    
    // Wait longer than the autocommit time
    assertTrue(trigger.waitForNewSearcher(45000));
    trigger.reset();
    
    req.setContentStreams( toContentStreams(
      adoc("id", "531", "field_t", "what's inside?", "subject", "info"), null ) );
    handler.handleRequest( req, rsp );
    assertEquals( 3, tracker.getCommitCount() );

    assertQ("now it should", req("id:500") ,"//result[@numFound=1]" );
    assertQ("but not this", req("id:531") ,"//result[@numFound=0]" );
  }

  public void testSoftCommitMaxDocs() throws Exception {

    SolrCore core = h.getCore();
    NewSearcherListener trigger = new NewSearcherListener();

    DirectUpdateHandler2 updateHandler = (DirectUpdateHandler2)core.getUpdateHandler();
    CommitTracker tracker = updateHandler.commitTracker;
    tracker.setTimeUpperBound(-1);
    tracker.setDocsUpperBound(8);
 
    
    NewSearcherListener softTrigger = new NewSearcherListener();

    CommitTracker softTracker = updateHandler.softCommitTracker;
    softTracker.setTimeUpperBound(-1);
    softTracker.setDocsUpperBound(4);
    core.registerNewSearcherListener(softTrigger);
    
    XmlUpdateRequestHandler handler = new XmlUpdateRequestHandler();
    handler.init( null );
    
    MapSolrParams params = new MapSolrParams( new HashMap<String, String>() );
    
    // Add documents
    SolrQueryResponse rsp = new SolrQueryResponse();
    SolrQueryRequestBase req = new SolrQueryRequestBase( core, params ) {};
    for( int i=0; i<4; i++ ) {
      req.setContentStreams( toContentStreams(
        adoc("id", Integer.toString(i), "subject", "info" ), null ) );
      handler.handleRequest( req, rsp );
    }
    // It should not be there right away
    assertQ("shouldn't find any", req("id:1") ,"//result[@numFound=0]" );
    assertEquals( 0, tracker.getCommitCount());

    req.setContentStreams( toContentStreams(
        adoc("id", "4", "subject", "info" ), null ) );
    handler.handleRequest( req, rsp );

    assertTrue(softTrigger.waitForNewSearcher(10000));
    
    core.registerNewSearcherListener(trigger);
    
    assertQ("should find 5", req("*:*") ,"//result[@numFound=5]" );
    assertEquals( 1, softTracker.getCommitCount());
    assertEquals( 0, tracker.getCommitCount());
    
    req.setContentStreams( toContentStreams(
        adoc("id", "5", "subject", "info" ), null ) );
    handler.handleRequest( req, rsp );
      
    // Now make sure we can find it
    assertQ("should find one", req("id:4") ,"//result[@numFound=1]" );
    assertEquals( 1, softTracker.getCommitCount());
    // But not the one added afterward
    assertQ("should not find one", req("id:5") ,"//result[@numFound=0]" );
    assertEquals( 1, softTracker.getCommitCount());
    
    for( int i=6; i<10; i++ ) {
      req.setContentStreams( toContentStreams(
        adoc("id", Integer.toString(i), "subject", "info" ), null ) );
      handler.handleRequest( req, rsp );
    }
    req.close();
    
    
    assertTrue(trigger.waitForNewSearcher(10000));
    assertQ("should find 10", req("*:*") ,"//result[@numFound=10]" );
    assertEquals( 2, softTracker.getCommitCount());
    assertEquals( 1, tracker.getCommitCount());
  }
  
  public void testSoftCommitMaxTime() throws Exception {
    SolrCore core = h.getCore();
    NewSearcherListener trigger = new NewSearcherListener();    
    core.registerNewSearcherListener(trigger);
    DirectUpdateHandler2 updater = (DirectUpdateHandler2) core.getUpdateHandler();
    CommitTracker tracker = updater.commitTracker;
    CommitTracker softTracker = updater.softCommitTracker;
    
    // too low of a number can cause a slow host to commit before the test code checks that it
    // isn't there... causing a failure at "shouldn't find any"
    softTracker.setTimeUpperBound(2000);
    softTracker.setDocsUpperBound(-1);
    // updater.commitCallbacks.add(trigger);
    
    XmlUpdateRequestHandler handler = new XmlUpdateRequestHandler();
    handler.init( null );
    
    MapSolrParams params = new MapSolrParams( new HashMap<String, String>() );
    
    // Add a single document
    SolrQueryResponse rsp = new SolrQueryResponse();
    SolrQueryRequestBase req = new SolrQueryRequestBase( core, params ) {};
    req.setContentStreams( toContentStreams(
      adoc("id", "529", "field_t", "what's inside?", "subject", "info"), null ) );
    trigger.reset();
    handler.handleRequest( req, rsp );

    // Check it it is in the index
    assertQ("shouldn't find any", req("id:529") ,"//result[@numFound=0]" );

    // Wait longer than the autocommit time
    assertTrue(trigger.waitForNewSearcher(30000));
    trigger.reset();
    req.setContentStreams( toContentStreams(
      adoc("id", "530", "field_t", "what's inside?", "subject", "info"), null ) );
    handler.handleRequest( req, rsp );
      
    // Now make sure we can find it
    assertQ("should find one", req("id:529") ,"//result[@numFound=1]" );
    // But not this one
    assertQ("should find none", req("id:530") ,"//result[@numFound=0]" );
    
    // Delete the document
    assertU( delI("529") );
    assertQ("deleted, but should still be there", req("id:529") ,"//result[@numFound=1]" );
    // Wait longer than the autocommit time
    assertTrue(trigger.waitForNewSearcher(15000));
    trigger.reset();
    req.setContentStreams( toContentStreams(
      adoc("id", "550", "field_t", "what's inside?", "subject", "info"), null ) );
    handler.handleRequest( req, rsp );
    assertEquals( 2, softTracker.getCommitCount() );
    assertQ("deleted and time has passed", req("id:529") ,"//result[@numFound=0]" );
    
    // now make the call 5 times really fast and make sure it 
    // only commits once
    req.setContentStreams( toContentStreams(
        adoc("id", "500" ), null ) );
    for( int i=0;i<5; i++ ) {
      handler.handleRequest( req, rsp );
    }
    assertQ("should not be there yet", req("id:500") ,"//result[@numFound=0]" );
    
    // Wait longer than the autocommit time
    assertTrue(trigger.waitForNewSearcher(15000));
    trigger.reset();
    
    req.setContentStreams( toContentStreams(
      adoc("id", "531", "field_t", "what's inside?", "subject", "info"), null ) );
    handler.handleRequest( req, rsp );
    assertEquals( 3, softTracker.getCommitCount() );
    assertEquals( 0, tracker.getCommitCount() );
    
    assertQ("now it should", req("id:500") ,"//result[@numFound=1]" );
    assertQ("but not this", req("id:531") ,"//result[@numFound=0]" );
  }
  
  public void testSoftAndHardCommitMaxTime() throws Exception {
    SolrCore core = h.getCore();
    NewSearcherListener trigger = new NewSearcherListener();    
    core.registerNewSearcherListener(trigger);
    DirectUpdateHandler2 updater = (DirectUpdateHandler2) core.getUpdateHandler();
    CommitTracker hardTracker = updater.commitTracker;
    CommitTracker softTracker = updater.softCommitTracker;
    
    // too low of a number can cause a slow host to commit before the test code checks that it
    // isn't there... causing a failure at "shouldn't find any"
    softTracker.setTimeUpperBound(200);
    softTracker.setDocsUpperBound(-1);
    hardTracker.setTimeUpperBound(1000);
    hardTracker.setDocsUpperBound(-1);
    // updater.commitCallbacks.add(trigger);
    
    XmlUpdateRequestHandler handler = new XmlUpdateRequestHandler();
    handler.init( null );
    
    MapSolrParams params = new MapSolrParams( new HashMap<String, String>() );
    
    // Add a single document
    SolrQueryResponse rsp = new SolrQueryResponse();
    SolrQueryRequestBase req = new SolrQueryRequestBase( core, params ) {};
    req.setContentStreams( toContentStreams(
      adoc("id", "529", "field_t", "what's inside?", "subject", "info"), null ) );
    trigger.reset();
    handler.handleRequest( req, rsp );

    // Check it it is in the index
    assertQ("shouldn't find any", req("id:529") ,"//result[@numFound=0]" );

    // Wait longer than the autocommit time
    assertTrue(trigger.waitForNewSearcher(30000));
    trigger.reset();
    req.setContentStreams( toContentStreams(
      adoc("id", "530", "field_t", "what's inside?", "subject", "info"), null ) );
    handler.handleRequest( req, rsp );
      
    // Now make sure we can find it
    assertQ("should find one", req("id:529") ,"//result[@numFound=1]" );
    // But not this one
    assertQ("should find none", req("id:530") ,"//result[@numFound=0]" );
    
    // Delete the document
    assertU( delI("529") );
    assertQ("deleted, but should still be there", req("id:529") ,"//result[@numFound=1]" );
    // Wait longer than the autocommit time
    assertTrue(trigger.waitForNewSearcher(15000));
    trigger.reset();
    req.setContentStreams( toContentStreams(
      adoc("id", "550", "field_t", "what's inside?", "subject", "info"), null ) );
    handler.handleRequest( req, rsp );
    assertEquals( 2, softTracker.getCommitCount() );
    assertQ("deleted and time has passed", req("id:529") ,"//result[@numFound=0]" );
    
    // now make the call 5 times really fast and make sure it 
    // only commits once
    req.setContentStreams( toContentStreams(
        adoc("id", "500" ), null ) );
    for( int i=0;i<5; i++ ) {
      handler.handleRequest( req, rsp );
    }
    assertQ("should not be there yet", req("id:500") ,"//result[@numFound=0]" );
    
    // Wait longer than the autocommit time
    assertTrue(trigger.waitForNewSearcher(15000));
    trigger.reset();
    
    req.setContentStreams( toContentStreams(
      adoc("id", "531", "field_t", "what's inside?", "subject", "info"), null ) );
    handler.handleRequest( req, rsp );
    
    // depending on timing, you might see 2 or 3 soft commits
    int softCommitCnt = softTracker.getCommitCount();
    assertTrue("commit cnt:" + softCommitCnt, softCommitCnt == 2
        || softCommitCnt == 3);
    assertEquals(1, hardTracker.getCommitCount());
    
    assertQ("now it should", req("id:500") ,"//result[@numFound=1]" );
    assertQ("but not this", req("id:531") ,"//result[@numFound=0]" );
  }
}
