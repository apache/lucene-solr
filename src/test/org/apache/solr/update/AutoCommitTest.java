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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.XmlUpdateRequestHandler;
import org.apache.solr.request.ContentStream;
import org.apache.solr.request.MapSolrParams;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.request.SolrQueryResponse;
import org.apache.solr.util.AbstractSolrTestCase;

/**
 * 
 * @author ryan
 *
 */
public class AutoCommitTest extends AbstractSolrTestCase {

  public String getSchemaFile() { return "schema.xml"; }
  public String getSolrConfigFile() { return "solrconfig.xml"; }
  
  /**
	 * Take a string and make it an iterable ContentStream
	 * 
	 * This should be moved to a helper class. (it is useful for the client too!)
	 */
	public static Collection<ContentStream> toContentStreams( final String str, final String contentType )
	{
		ArrayList<ContentStream> streams = new ArrayList<ContentStream>();
		streams.add( new ContentStream() {
			public String getContentType() { return contentType; }
			public Long getSize() { return Long.valueOf( str.length() ); }
			public String getName() { return null; }
      public String getSourceInfo() { return null; }
	
			public InputStream getStream() throws IOException {
				return new ByteArrayInputStream( str.getBytes() );
			}
		});
		return streams;
	}

  public void testMaxDocs() throws Exception {
    
    DirectUpdateHandler2 updater = (DirectUpdateHandler2)SolrCore.getSolrCore().getUpdateHandler();
    DirectUpdateHandler2.CommitTracker tracker = updater.tracker;
    tracker.timeUpperBound = -1;
    tracker.docsUpperBound = 5;
    
    XmlUpdateRequestHandler handler = new XmlUpdateRequestHandler();
    handler.init( null );
    
    SolrCore core = SolrCore.getSolrCore();
    MapSolrParams params = new MapSolrParams( new HashMap<String, String>() );
    
    // Add a single document
    SolrQueryResponse rsp = new SolrQueryResponse();
    SolrQueryRequestBase req = new SolrQueryRequestBase( core, params ) {};
    for( int i=0; i<15; i++ ) {
      req.setContentStreams( toContentStreams(
        adoc("id", "A"+i, "subject", "info" ), null ) );
      handler.handleRequest( req, rsp );
    }
    // It should not be there right away
    assertQ("shouldn't find any", req("id:A1") ,"//result[@numFound=0]" );
    assertEquals( 0, tracker.autoCommitCount );

    // Wait longer then the autocommit time
    Thread.sleep( 500 );
      
    // Now make sure we can find it
    assertQ("should find one", req("id:A1") ,"//result[@numFound=1]" );
    assertEquals( 1, tracker.autoCommitCount );
    
    // Now add some more
    for( int i=0; i<15; i++ ) {
      req.setContentStreams( toContentStreams(
        adoc("id", "B"+i, "subject", "info" ), null ) );
      handler.handleRequest( req, rsp );
    }
    // It should not be there right away
    assertQ("shouldn't find any", req("id:B1") ,"//result[@numFound=0]" );
    assertEquals( 1, tracker.autoCommitCount );
    
    Thread.sleep( 500 );
    assertQ("should find one", req("id:B1") ,"//result[@numFound=1]" );
    assertEquals( 2, tracker.autoCommitCount );
  }

  public void testMaxTime() throws Exception {
    
    DirectUpdateHandler2 updater = (DirectUpdateHandler2)SolrCore.getSolrCore().getUpdateHandler();
    DirectUpdateHandler2.CommitTracker tracker = updater.tracker;
    tracker.timeUpperBound = 500;
    tracker.docsUpperBound = -1;
    
    XmlUpdateRequestHandler handler = new XmlUpdateRequestHandler();
    handler.init( null );
    
    SolrCore core = SolrCore.getSolrCore();
    MapSolrParams params = new MapSolrParams( new HashMap<String, String>() );
    
    // Add a single document
    SolrQueryResponse rsp = new SolrQueryResponse();
    SolrQueryRequestBase req = new SolrQueryRequestBase( core, params ) {};
    req.setContentStreams( toContentStreams(
      adoc("id", "529",
           "field_t", "what's inside?",
           "subject", "info"
      ), null ) );
    handler.handleRequest( req, rsp );

    // Check it it is in the index
    assertQ("shouldn't find any", req("id:529") ,"//result[@numFound=0]" );

    // Wait longer then the autocommit time
    Thread.sleep( 1000 );
      
    // Now make sure we can find it
    assertQ("should find one", req("id:529") ,"//result[@numFound=1]" );
    
    // now make the call 10 times really fast and make sure it 
    // only commits once
    req.setContentStreams( toContentStreams(
	      adoc("id", "500" ), null ) );
    for( int i=0;i<10; i++ ) {
    	handler.handleRequest( req, rsp );
    }
    assertQ("should not be there yet", req("id:500") ,"//result[@numFound=0]" );
    assertEquals( 1, tracker.autoCommitCount );
    
    // Wait longer then the autocommit time
    Thread.sleep( 1000 );

    assertQ("now it should", req("id:500") ,"//result[@numFound=1]" );
    assertEquals( 2, tracker.autoCommitCount );
  }
}
