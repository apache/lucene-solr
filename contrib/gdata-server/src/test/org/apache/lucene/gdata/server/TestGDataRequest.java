/** 
 * Copyright 2004 The Apache Software Foundation 
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at 
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0 
 * 
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and 
 * limitations under the License. 
 */ 
package org.apache.lucene.gdata.server; 
 
import javax.servlet.http.HttpServletRequest;

import junit.framework.TestCase;

import org.apache.lucene.gdata.server.GDataRequest.GDataRequestType;
import org.apache.lucene.gdata.server.GDataRequest.OutputFormat;
import org.apache.lucene.gdata.server.registry.GDataServerRegistry;
import org.apache.lucene.gdata.server.registry.ProvidedService;
import org.apache.lucene.gdata.server.registry.RegistryException;
import org.apache.lucene.gdata.utils.ProvidedServiceStub;
import org.apache.lucene.gdata.utils.StorageStub;
import org.easymock.MockControl;
 
/** 
 *  
 * @author Simon Willnauer 
 *  
 */ 
public class TestGDataRequest extends TestCase { 
    private HttpServletRequest request; 
 
    private MockControl control; 
 
    private GDataRequest feedRequest; 
    static{
        try {
            GDataServerRegistry.getRegistry().registerComponent(StorageStub.class);
        } catch (RegistryException e) {
            
            e.printStackTrace();
        }
    }
    
    @Override 
    protected void setUp() throws Exception { 
        ProvidedService configurator = new ProvidedServiceStub();
        GDataServerRegistry.getRegistry().registerService(configurator); 
        
            
        this.control = MockControl.createControl(HttpServletRequest.class); 
        this.request = (HttpServletRequest) this.control.getMock(); 
        this.feedRequest = new GDataRequest(this.request,GDataRequestType.GET); 
         
    } 
 
    protected void tearDown() throws Exception { 
        super.tearDown(); 
        this.control.reset(); 
    } 
 
    public void testConstructor() { 
        try { 
            new GDataRequest(null,GDataRequestType.GET); 
            fail("IllegalArgumentException expected"); 
        } catch (IllegalArgumentException e) { 
            //  
        } 
        try { 
            new GDataRequest(null,null); 
            fail("IllegalArgumentException expected"); 
        } catch (IllegalArgumentException e) { 
            //  
        } 
        try { 
            new GDataRequest(this.request,null); 
            fail("IllegalArgumentException expected"); 
        } catch (IllegalArgumentException e) { 
            //  
        } 
    } 
 
    public void testGetFeedId() throws GDataRequestException { 
 
        this.control.expectAndDefaultReturn(this.request.getPathInfo(), 
                "/feed/1/1"); 
        this.control.expectAndDefaultReturn(this.request.getParameter("alt"), 
                null); 
        this.control.replay(); 
        this.feedRequest.initializeRequest(); 
        assertEquals("feedID", this.feedRequest.getFeedId(), "feed"); 
        this.control.reset(); 
 
    } 
 
    public void testEmptyPathInfo() { 
        this.control.expectAndDefaultReturn(this.request.getPathInfo(), "/"); 
        this.control.expectAndDefaultReturn(this.request.getParameter("alt"), 
                null); 
        this.control.replay(); 
        try { 
            this.feedRequest.initializeRequest(); 
 
            fail("FeedRequestException expected"); 
        } catch (GDataRequestException e) { 
            // expected 
        } catch (Exception e) { 
            fail("FeedRequestException expected"); 
        } 
 
    } 
 
    public void testGetFeedIdWithoutEntry() throws GDataRequestException { 
        this.control 
                .expectAndDefaultReturn(this.request.getPathInfo(), "/feed"); 
        this.control.expectAndDefaultReturn(this.request.getParameter("alt"), 
                null); 
        this.control.replay(); 
        this.feedRequest.initializeRequest(); 
        assertEquals("feedID", this.feedRequest.getFeedId(), "feed"); 
    } 
 
    public void testGetEntyId() throws GDataRequestException { 
 
        this.control.expectAndDefaultReturn(this.request.getPathInfo(), 
                "/feed/1/15"); 
        this.control.expectAndDefaultReturn(this.request.getParameter("alt"), 
                null); 
        this.control.replay(); 
        this.feedRequest.initializeRequest(); 
        assertEquals("entryid", this.feedRequest.getEntryId(), "1"); 
        assertEquals("feedId", this.feedRequest.getFeedId(), "feed"); 
        assertEquals("entryid", this.feedRequest.getEntryVersion(), "15"); 
        this.control.reset(); 
 
    } 
 
    public void testSetResponseFormatAtom() throws GDataRequestException { 
        this.control.expectAndDefaultReturn(this.request.getParameter("alt"), 
                "atom"); 
        this.control 
                .expectAndDefaultReturn(this.request.getPathInfo(), "/feed"); 
        this.control.replay(); 
        this.feedRequest.initializeRequest(); 
        assertEquals("ResponseFromat Atom", this.feedRequest 
                .getRequestedResponseFormat(), OutputFormat.ATOM); 
        this.control.reset(); 
    } 
 
    public void testSetResponseFormatRSS() throws GDataRequestException { 
        this.control.expectAndDefaultReturn(this.request.getParameter("alt"), 
                "rss"); 
        this.control 
                .expectAndDefaultReturn(this.request.getPathInfo(), "/feed"); 
        this.control.replay(); 
        this.feedRequest.initializeRequest(); 
        assertEquals("ResponseFromat RSS", this.feedRequest 
                .getRequestedResponseFormat(), OutputFormat.RSS); 
        this.control.reset(); 
    } 
 
    public void testSetResponseFormatKeepAtom() throws GDataRequestException { 
        this.control.expectAndDefaultReturn(this.request.getParameter("alt"), 
                "fooBar"); 
        this.control 
                .expectAndDefaultReturn(this.request.getPathInfo(), "/feed"); 
        this.control.replay(); 
        this.feedRequest.initializeRequest(); 
        assertEquals("ResponseFromat Atom", this.feedRequest 
                .getRequestedResponseFormat(), OutputFormat.ATOM); 
        this.control.reset(); 
    } 
 
    public void testSetResponseFormatNull() throws GDataRequestException { 
        this.control.expectAndDefaultReturn(this.request.getParameter("alt"), 
                null); 
 
        this.control 
                .expectAndDefaultReturn(this.request.getPathInfo(), "/feed"); 
        this.control.replay(); 
        this.feedRequest.initializeRequest(); 
        assertEquals("ResponseFromat Atom", this.feedRequest 
                .getRequestedResponseFormat(), OutputFormat.ATOM); 
        this.control.reset(); 
    } 
 
    public void testGetItemsPerPage() throws GDataRequestException { 
        this.control.expectAndReturn(this.request.getParameter("max-results"), 
                null); 
        this.control.replay(); 
        assertEquals("default value 25", 25, this.feedRequest.getItemsPerPage()); 
        this.control.verify(); 
        this.control.reset(); 
 
        this.control.expectAndReturn(this.request.getParameter("max-results"), 
                "24", 2); 
        this.control.replay(); 
        assertEquals("24 results", 24, this.feedRequest.getItemsPerPage()); 
        this.control.verify(); 
        this.control.reset(); 
         
        this.control.expectAndReturn(this.request.getParameter("max-results"), 
                "-1", 2); 
        this.control.replay(); 
        assertEquals("25 results", 25, this.feedRequest.getItemsPerPage()); 
        this.control.verify(); 
        this.control.reset(); 
 
        this.control.expectAndReturn(this.request.getParameter("max-results"), 
                "helloworld", 2); 
        this.control.replay(); 
        assertEquals("25 results", 25, this.feedRequest.getItemsPerPage()); 
        this.control.verify(); 
        this.control.reset(); 
    } 
     
    public void testGetSelfId() throws GDataRequestException{ 
        String host = "www.apache.org"; 
        String feedAndEntryID = "/feed/entryid"; 
        String queryString = "max-results=25"; 
        this.control.expectAndDefaultReturn(this.request.getHeader("Host"),host); 
        this.control.expectAndDefaultReturn(this.request.getRequestURI(),"/host/feed/entryId/15"); 
        this.control.expectAndDefaultReturn(this.request.getPathInfo(),"/feed/entryId/15"); 
        this.control.expectAndReturn(this.request.getParameter("max-results"),"25",2); 
        this.control.expectAndDefaultReturn(this.request.getParameter("alt"), 
                null); 
        this.control.expectAndDefaultReturn(this.request.getQueryString(), 
                queryString); 
        this.control.replay(); 
        this.feedRequest.initializeRequest(); 
        String selfID = "http://"+host+"/host/feed/entryId/15?"+queryString; 
     
        assertEquals("Self ID",selfID,this.feedRequest.getSelfId()); 
        this.control.reset(); 
         
         
        queryString = "alt=rss&max-results=25"; 
        this.control.expectAndDefaultReturn(this.request.getHeader("Host"),host); 
        this.control.expectAndDefaultReturn(this.request.getRequestURI(),"/host/feed/entryId/15"); 
        this.control.expectAndDefaultReturn(this.request.getPathInfo(),"/feed/entryId/15"); 
        this.control.expectAndReturn(this.request.getParameter("max-results"),"25",2); 
        this.control.expectAndDefaultReturn(this.request.getParameter("alt"), 
                null); 
        this.control.expectAndDefaultReturn(this.request.getQueryString(), 
                queryString); 
        this.control.replay(); 
        this.feedRequest.initializeRequest(); 
        selfID = "http://"+host+"/host/feed/entryId/15?"+queryString; 
     
        assertEquals("Self ID",selfID,this.feedRequest.getSelfId()); 
        this.control.reset(); 
         
        queryString = ""; 
        this.control.expectAndDefaultReturn(this.request.getHeader("Host"),host); 
        this.control.expectAndDefaultReturn(this.request.getRequestURI(),"/host/feed/entryId/15"); 
        this.control.expectAndDefaultReturn(this.request.getPathInfo(),"/feed/entryId/15"); 
        this.control.expectAndDefaultReturn(this.request.getParameter("max-results"),null); 
        this.control.expectAndDefaultReturn(this.request.getParameter("alt"), 
                null); 
        this.control.expectAndDefaultReturn(this.request.getQueryString(), 
                null); 
        this.control.replay(); 
        this.feedRequest.initializeRequest(); 
        selfID = "http://"+host+"/host/feed/entryId/15"+"?max-results=25"; 
     
        assertEquals("Self ID",selfID,this.feedRequest.getSelfId()); 
        this.control.reset(); 
    } 
     
    public void testGetQueryString(){ 
        String maxResults  = "max-results=25"; 
        String queryString = "?"+maxResults; 
        this.control.expectAndReturn(this.request.getParameter("max-results"),"25",2); 
         
        this.control.expectAndDefaultReturn(this.request.getQueryString(), 
                queryString); 
        this.control.replay(); 
         
        assertEquals(queryString,this.feedRequest.getQueryString()); 
        this.control.reset(); 
        // test no result defined 
        queryString = "?alt=rss"; 
        this.control.expectAndDefaultReturn(this.request.getParameter("max-results"),null); 
         
        this.control.expectAndDefaultReturn(this.request.getQueryString(), 
                queryString); 
        this.control.replay(); 
         
        assertEquals(queryString+"&"+maxResults,this.feedRequest.getQueryString()); 
        this.control.reset(); 
         
//         test no result defined && query == null 
        queryString = null; 
        this.control.expectAndDefaultReturn(this.request.getParameter("max-results"),null); 
         
        this.control.expectAndDefaultReturn(this.request.getQueryString(), 
                queryString); 
        this.control.replay(); 
         
        assertEquals(maxResults,this.feedRequest.getQueryString()); 
        this.control.reset(); 
     
    } 
     
    public void testIsFeedRequest() throws GDataRequestException{ 
        String host = "www.apache.org"; 
        String feedAndEntryID = "/feed"; 
         
        this.control.expectAndDefaultReturn(this.request.getHeader("Host"),host); 
        this.control.expectAndDefaultReturn(this.request.getRequestURI(),"/host/feed"); 
        this.control.expectAndDefaultReturn(this.request.getPathInfo(),"/feed"); 
         
        this.control.expectAndDefaultReturn(this.request.getParameter("alt"), 
                null); 
        this.control.expectAndDefaultReturn(this.request.getQueryString(), 
                null); 
        this.control.replay(); 
        this.feedRequest.initializeRequest(); 
         
     
        assertTrue(this.feedRequest.isFeedRequested()); 
        assertFalse(this.feedRequest.isEntryRequested()); 
        this.control.reset(); 
         
        host = "www.apache.org"; 
        feedAndEntryID = "/feed/1"; 
         
        this.control.expectAndDefaultReturn(this.request.getHeader("Host"),host); 
        this.control.expectAndDefaultReturn(this.request.getRequestURI(),"/host/feed/1"); 
        this.control.expectAndDefaultReturn(this.request.getPathInfo(),feedAndEntryID); 
         
        this.control.expectAndDefaultReturn(this.request.getParameter("alt"), 
                null); 
        this.control.expectAndDefaultReturn(this.request.getQueryString(), 
                null); 
        this.control.replay(); 
        this.feedRequest.initializeRequest(); 
         
     
        assertFalse(this.feedRequest.isFeedRequested()); 
        assertTrue(this.feedRequest.isEntryRequested()); 
        this.control.reset(); 
         
         
    } 
    public void testgetAuthToken(){ 
        this.control.expectAndDefaultReturn(this.request.getHeader("Authentication"),"GoogleLogin auth=bla");
        this.control.replay();
        assertEquals("bla",this.feedRequest.getAuthToken());
        this.control.verify();
        
    } 
     
    public void testGetNextId() throws GDataRequestException{ 
//        String host = "www.apache.org"; 
//        String feedAndEntryID = "/feed/entryid"; 
//        String queryString = "?max-results=25"; 
//        String startIndex = "&start-index=26"; 
//        this.control.expectAndDefaultReturn(this.request.getHeader("Host"),host); 
//        this.control.expectAndDefaultReturn(this.request.getRequestURI(),"/feed/"); 
//        this.control.expectAndDefaultReturn(this.request.getPathInfo(),"/feed/"); 
//        this.control.expectAndReturn(this.request.getParameter("max-results"),"25",2); 
//        this.control.expectAndReturn(this.request.getParameter("start-index"),null); 
//        this.control.expectAndDefaultReturn(this.request.getParameter("alt"), 
//                null); 
//        this.control.expectAndDefaultReturn(this.request.getQueryString(), 
//                queryString); 
//        this.control.replay(); 
//        this.feedRequest.initializeRequest(); 
//        String nextID = "http://"+host+"/feed/"+queryString+startIndex; 
//     
//        assertEquals("Next ID",nextID,this.feedRequest.getNextId()); 
//        this.control.reset(); 
         
         
//        queryString = "?alt=rss&max-results=25"; 
//         
//        this.control.expectAndDefaultReturn(this.request.getHeader("Host"),host); 
//        this.control.expectAndDefaultReturn(this.request.getRequestURI(),"/feed/"); 
//        this.control.expectAndDefaultReturn(this.request.getPathInfo(),"/feed/"); 
//        this.control.expectAndReturn(this.request.getParameter("max-results"),"25",2); 
//        this.control.expectAndReturn(this.request.getParameter("start-index"),"26",2); 
//        this.control.expectAndDefaultReturn(this.request.getParameter("alt"), 
//                null); 
//        this.control.expectAndDefaultReturn(this.request.getQueryString(), 
//                queryString+startIndex); 
//        Enumeration e = 
//        this.control.expectAndDefaultReturn(this.request.getParameterNames(),)
//        
//        
//        this.control.replay(); 
//        this.feedRequest.initializeRequest(); 
//        startIndex = "&start-index=51"; 
//        nextID = "http://"+host+"/feed"+queryString+startIndex; 
//     
//        assertEquals("Next ID 51",nextID,this.feedRequest.getNextId()); 
//        this.control.reset(); 
//         
//        queryString = ""; 
//        this.control.expectAndDefaultReturn(this.request.getHeader("Host"),host); 
//        this.control.expectAndDefaultReturn(this.request.getPathInfo(),"/feed/entryId/15"); 
//        this.control.expectAndDefaultReturn(this.request.getParameter("max-results"),null); 
//        this.control.expectAndDefaultReturn(this.request.getParameter("alt"), 
//                null); 
//        this.control.expectAndDefaultReturn(this.request.getQueryString(), 
//                null); 
//        this.control.replay(); 
//        this.feedRequest.initializeRequest(); 
//        String selfID = "http://"+host+"/feed"+"?max-results=25"; 
//     
//        assertEquals("Self ID",selfID,this.feedRequest.getSelfId()); 
//        this.control.reset(); 
    } 
} 
