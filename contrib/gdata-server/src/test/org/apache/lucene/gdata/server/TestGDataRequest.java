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
 
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import javax.servlet.http.HttpServletRequest;

import junit.framework.TestCase;

import org.apache.lucene.gdata.search.config.IndexSchema;
import org.apache.lucene.gdata.search.config.IndexSchemaField;
import org.apache.lucene.gdata.search.config.IndexSchemaField.ContentType;
import org.apache.lucene.gdata.server.GDataRequest.GDataRequestType;
import org.apache.lucene.gdata.server.GDataRequest.OutputFormat;
import org.apache.lucene.gdata.server.registry.GDataServerRegistry;
import org.apache.lucene.gdata.server.registry.ProvidedService;
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
    
    private Map parametermap = new HashMap();
    
    @Override 
    protected void setUp() throws Exception {
        try{
            GDataServerRegistry.getRegistry().registerComponent(StorageStub.class,null);
        }catch (Exception e) {

        }
        ProvidedService configurator = new ProvidedServiceStub();
        GDataServerRegistry.getRegistry().registerService(configurator);
        IndexSchema schema = new IndexSchema();
        //must be set
        schema.setDefaultSearchField("field");
        schema.setIndexLocation("/tmp/");
        schema.setName(ProvidedServiceStub.SERVICE_NAME);
        
        ((ProvidedServiceStub)configurator).setIndexSchema(schema);
            
        this.control = MockControl.createControl(HttpServletRequest.class); 
        this.request = (HttpServletRequest) this.control.getMock(); 
        this.feedRequest = new GDataRequest(this.request,GDataRequestType.GET); 
         
    } 
 
    protected void tearDown() throws Exception { 
        super.tearDown(); 
        this.control.reset(); 
        GDataServerRegistry.getRegistry().destroy();
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
        this.control.expectAndDefaultReturn(this.request.getParameterMap(),this.parametermap);
        this.control.expectAndDefaultReturn(this.request.getPathInfo(), 
                "/"+ProvidedServiceStub.SERVICE_NAME+"/feed/1/1"); 
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
        this.control.expectAndDefaultReturn(this.request.getParameterMap(),this.parametermap);
        this.control 
                .expectAndDefaultReturn(this.request.getPathInfo(), "/"+ProvidedServiceStub.SERVICE_NAME+"/feed"); 
        this.control.expectAndDefaultReturn(this.request.getParameter("alt"), 
                null); 
        this.control.replay(); 
        this.feedRequest.initializeRequest(); 
        assertEquals("feedID", this.feedRequest.getFeedId(), "feed"); 
    } 
 
    public void testGetEntyId() throws GDataRequestException { 
        this.control.expectAndDefaultReturn(this.request.getParameterMap(),this.parametermap);
        this.control.expectAndDefaultReturn(this.request.getPathInfo(), 
                "/"+ProvidedServiceStub.SERVICE_NAME+"/feed/1/15"); 
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
        this.control.expectAndDefaultReturn(this.request.getParameterMap(),this.parametermap);
        this.control.expectAndDefaultReturn(this.request.getParameter("alt"), 
                "atom"); 
        this.control 
                .expectAndDefaultReturn(this.request.getPathInfo(),"/"+ProvidedServiceStub.SERVICE_NAME+ "/feed"); 
        this.control.replay(); 
        this.feedRequest.initializeRequest(); 
        assertEquals("ResponseFromat Atom", this.feedRequest 
                .getRequestedResponseFormat(), OutputFormat.ATOM); 
        this.control.reset(); 
    } 
 
    public void testSetResponseFormatRSS() throws GDataRequestException { 
        this.control.expectAndDefaultReturn(this.request.getParameterMap(),this.parametermap);
        this.control.expectAndDefaultReturn(this.request.getParameter("alt"), 
                "rss"); 
        this.control 
                .expectAndDefaultReturn(this.request.getPathInfo(), "/"+ProvidedServiceStub.SERVICE_NAME+"/feed"); 
        this.control.replay(); 
        this.feedRequest.initializeRequest(); 
        assertEquals("ResponseFromat RSS", this.feedRequest 
                .getRequestedResponseFormat(), OutputFormat.RSS); 
        this.control.reset(); 
    } 
 
    public void testSetResponseFormatKeepAtom() throws GDataRequestException { 
        this.control.expectAndDefaultReturn(this.request.getParameterMap(),this.parametermap);
        this.control.expectAndDefaultReturn(this.request.getParameter("alt"), 
                "fooBar"); 
        this.control 
                .expectAndDefaultReturn(this.request.getPathInfo(), "/"+ProvidedServiceStub.SERVICE_NAME+"/feed"); 
        this.control.replay(); 
        this.feedRequest.initializeRequest(); 
        assertEquals("ResponseFromat Atom", this.feedRequest 
                .getRequestedResponseFormat(), OutputFormat.ATOM); 
        this.control.reset(); 
    } 
 
    public void testSetResponseFormatNull() throws GDataRequestException { 
        this.control.expectAndDefaultReturn(this.request.getParameterMap(),this.parametermap);
        this.control.expectAndDefaultReturn(this.request.getParameter("alt"), 
                null); 
 
        this.control 
                .expectAndDefaultReturn(this.request.getPathInfo(), "/"+ProvidedServiceStub.SERVICE_NAME+"/feed"); 
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
        String feedAndEntryID = "/"+ProvidedServiceStub.SERVICE_NAME+"/feed/entryid"; 
        String queryString = "max-results=25"; 
        this.control.expectAndDefaultReturn(this.request.getParameterMap(),this.parametermap);
        this.control.expectAndDefaultReturn(this.request.getHeader("Host"),host); 
        this.control.expectAndDefaultReturn(this.request.getRequestURI(),"/host"+"/"+ProvidedServiceStub.SERVICE_NAME+"/feed/entryId/15/"); 
        this.control.expectAndDefaultReturn(this.request.getPathInfo(),"/"+ProvidedServiceStub.SERVICE_NAME+"/feed/entryId/15"); 
        this.control.expectAndReturn(this.request.getParameter("max-results"),"25",2); 
        this.control.expectAndDefaultReturn(this.request.getParameter("alt"), 
                null); 
        this.control.expectAndDefaultReturn(this.request.getQueryString(), 
                queryString); 
        this.control.replay(); 
        this.feedRequest.initializeRequest(); 
        String selfID = "http://"+host+"/host"+"/"+ProvidedServiceStub.SERVICE_NAME+"/feed/entryId/15?"+queryString; 
        assertEquals("Self ID",selfID,this.feedRequest.getSelfId()); 
        this.control.reset(); 
         
         
        queryString = "alt=rss&max-results=25";
        this.control.expectAndDefaultReturn(this.request.getParameterMap(),this.parametermap);
        this.control.expectAndDefaultReturn(this.request.getHeader("Host"),host); 
        this.control.expectAndDefaultReturn(this.request.getRequestURI(),"/host"+"/"+ProvidedServiceStub.SERVICE_NAME+"/feed/entryId/15/"); 
        this.control.expectAndDefaultReturn(this.request.getPathInfo(),"/"+ProvidedServiceStub.SERVICE_NAME+"/feed/entryId/15"); 
        this.control.expectAndReturn(this.request.getParameter("max-results"),"25",2); 
        this.control.expectAndDefaultReturn(this.request.getParameter("alt"), 
                null); 
        this.control.expectAndDefaultReturn(this.request.getQueryString(), 
                queryString); 
        this.control.replay(); 
        this.feedRequest.initializeRequest(); 
        selfID = "http://"+host+"/host"+"/"+ProvidedServiceStub.SERVICE_NAME+"/feed/entryId/15?"+queryString; 
     
        assertEquals("Self ID",selfID,this.feedRequest.getSelfId()); 
        this.control.reset(); 
         
        queryString = ""; 
        this.control.expectAndDefaultReturn(this.request.getParameterMap(),this.parametermap);
        this.control.expectAndDefaultReturn(this.request.getHeader("Host"),host); 
        this.control.expectAndDefaultReturn(this.request.getRequestURI(),"/host"+"/"+ProvidedServiceStub.SERVICE_NAME+"/feed/entryId/15/"); 
        this.control.expectAndDefaultReturn(this.request.getPathInfo(),"/"+ProvidedServiceStub.SERVICE_NAME+"/feed/entryId/15"); 
        this.control.expectAndDefaultReturn(this.request.getParameter("max-results"),null); 
        this.control.expectAndDefaultReturn(this.request.getParameter("alt"), 
                null); 
        this.control.expectAndDefaultReturn(this.request.getQueryString(), 
                null); 
        this.control.replay(); 
        this.feedRequest.initializeRequest(); 
        selfID = "http://"+host+"/host"+"/"+ProvidedServiceStub.SERVICE_NAME+"/feed/entryId/15"+"?max-results=25"; 
     
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
        this.control.expectAndDefaultReturn(this.request.getParameterMap(),this.parametermap);
        this.control.expectAndDefaultReturn(this.request.getHeader("Host"),host); 
        this.control.expectAndDefaultReturn(this.request.getRequestURI(),"/host"+"/"+ProvidedServiceStub.SERVICE_NAME+"/feed"); 
        this.control.expectAndDefaultReturn(this.request.getPathInfo(),"/"+ProvidedServiceStub.SERVICE_NAME+"/feed"); 
         
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
        feedAndEntryID = "/"+ProvidedServiceStub.SERVICE_NAME+"/feed/1"; 
        this.control.expectAndDefaultReturn(this.request.getParameterMap(),this.parametermap);
        this.control.expectAndDefaultReturn(this.request.getHeader("Host"),host); 
        this.control.expectAndDefaultReturn(this.request.getRequestURI(),"/host"+"/"+ProvidedServiceStub.SERVICE_NAME+"/feed/1"); 
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
        
        host = "www.apache.org"; 
        feedAndEntryID = "/"+ProvidedServiceStub.SERVICE_NAME+"/feed/1"; 
        this.control.expectAndDefaultReturn(this.request.getParameterMap(),this.parametermap);
        this.control.expectAndDefaultReturn(this.request.getHeader("Host"),host); 
        this.control.expectAndDefaultReturn(this.request.getRequestURI(),"/host"+"/"+"NOTREGISTERED"+"/feed/1"); 
        this.control.expectAndDefaultReturn(this.request.getPathInfo(),"/"+"NOTREGISTERED"+"/feed/1"); 
         
        this.control.expectAndDefaultReturn(this.request.getParameter("alt"), 
                null); 
        this.control.expectAndDefaultReturn(this.request.getQueryString(), 
                null); 
        this.control.replay(); 
        try{
        this.feedRequest.initializeRequest();
        fail("service not registered");
        }catch (GDataRequestException e) {
            //
        }
        this.control.reset(); 
        
        host = "www.apache.org"; 
        feedAndEntryID = "/"+ProvidedServiceStub.SERVICE_NAME+"/feed/1"; 
        this.control.expectAndDefaultReturn(this.request.getParameterMap(),this.parametermap);
        this.control.expectAndDefaultReturn(this.request.getHeader("Host"),host); 
        this.control.expectAndDefaultReturn(this.request.getRequestURI(),"/host"+"/"+ProvidedServiceStub.SERVICE_NAME+"/"); 
        this.control.expectAndDefaultReturn(this.request.getPathInfo(),"/"+ProvidedServiceStub.SERVICE_NAME+"/"); 
         
        this.control.expectAndDefaultReturn(this.request.getParameter("alt"), 
                null); 
        this.control.expectAndDefaultReturn(this.request.getQueryString(), 
                null); 
        this.control.replay(); 
        try{
        this.feedRequest.initializeRequest();
        fail("service not registered");
        }catch (GDataRequestException e) {
//
        }
        this.control.reset(); 
         
         
    } 
    public void testgetAuthToken(){ 
        this.control.expectAndDefaultReturn(this.request.getHeader("Authentication"),"GoogleLogin auth=bla");
        this.control.replay();
        assertEquals("bla",this.feedRequest.getAuthToken());
        this.control.verify();
        this.control.reset();
        
        this.control.expectAndDefaultReturn(this.request.getHeader("Authentication"),null);
        this.control.replay();
        assertNull(this.feedRequest.getAuthToken());
        this.control.verify();
        this.control.reset();
        
    } 
    
    public void testGetStartIndex(){
        this.control.expectAndDefaultReturn(this.request.getParameter("start-index"),"5");
        this.control.replay();
        assertEquals(5,this.feedRequest.getStartIndex());
        this.control.verify();
        this.control.reset();
        this.control.expectAndDefaultReturn(this.request.getParameter("start-index"),"-5");
        this.control.replay();
        assertEquals(1,this.feedRequest.getStartIndex());
        this.control.verify();
        this.control.reset();
        this.control.expectAndDefaultReturn(this.request.getParameter("start-index"),"unparsable");
        this.control.replay();
        assertEquals(1,this.feedRequest.getStartIndex());
        this.control.verify();
        this.control.reset();
    }
     
    public void testGetNextId() throws GDataRequestException{ 
        String host = "www.apache.org"; 
        String feedAndEntryID = "/"+ProvidedServiceStub.SERVICE_NAME+"/feed/entryid"; 
        String queryString = "max-results=25"; 
        String startIndex = "start-index=26"; 
        Enumeration enu = new StringTokenizer("max-results",",");
        
        this.control.expectAndDefaultReturn(this.request.getParameterMap(),this.parametermap);
        this.control.expectAndDefaultReturn(this.request.getParameterNames(),enu);
        this.control.expectAndDefaultReturn(this.request.getHeader("Host"),host); 
        this.control.expectAndDefaultReturn(this.request.getRequestURI(),"/"+ProvidedServiceStub.SERVICE_NAME+"/feed/"); 
        this.control.expectAndDefaultReturn(this.request.getPathInfo(),"/"+ProvidedServiceStub.SERVICE_NAME+"/feed/"); 
        this.control.expectAndReturn(this.request.getParameter("max-results"),"25",3); 
        this.control.expectAndReturn(this.request.getParameter("start-index"),null); 
        this.control.expectAndDefaultReturn(this.request.getParameter("alt"), 
                null); 
        this.control.expectAndDefaultReturn(this.request.getQueryString(), 
                queryString); 
        this.control.replay(); 

        this.feedRequest.initializeRequest(); 
        String nextID = "http://"+host+"/"+ProvidedServiceStub.SERVICE_NAME+"/feed?"+startIndex+"&"+queryString; 
     
        assertEquals("Next ID",nextID,this.feedRequest.getNextId());
        this.control.verify();
        this.control.reset(); 
         
        enu = new StringTokenizer("alt,max-results,start-index",",");
        queryString = "alt=rss&max-results=25"; 
        this.control.expectAndDefaultReturn(this.request.getParameterMap(),this.parametermap);
        this.control.expectAndDefaultReturn(this.request.getHeader("Host"),host); 
        this.control.expectAndDefaultReturn(this.request.getRequestURI(),"/"+ProvidedServiceStub.SERVICE_NAME+"/feed/"); 
        this.control.expectAndDefaultReturn(this.request.getPathInfo(),"/"+ProvidedServiceStub.SERVICE_NAME+"/feed/"); 
        this.control.expectAndReturn(this.request.getParameter("max-results"),"25",4); 
        this.control.expectAndReturn(this.request.getParameter("start-index"),"26",4); 
        this.control.expectAndDefaultReturn(this.request.getParameter("alt"), 
                "rss"); 
        this.control.expectAndDefaultReturn(this.request.getQueryString(), 
                queryString+"&"+startIndex); 
        
        this.control.expectAndDefaultReturn(this.request.getParameterNames(),enu);
        
        
        this.control.replay(); 
        this.feedRequest.initializeRequest(); 
        startIndex = "&start-index=51"; 
        nextID = "http://"+host+"/"+ProvidedServiceStub.SERVICE_NAME+"/feed?"+queryString+startIndex; 
     
        assertEquals("Next ID 51",nextID,this.feedRequest.getNextId()); 
        this.control.reset(); 
         
        queryString = ""; 
        this.control.expectAndDefaultReturn(this.request.getParameterMap(),this.parametermap);
        this.control.expectAndDefaultReturn(this.request.getHeader("Host"),host); 
        this.control.expectAndDefaultReturn(this.request.getRequestURI(),"/"+ProvidedServiceStub.SERVICE_NAME+"/feed/"); 
        this.control.expectAndDefaultReturn(this.request.getPathInfo(),"/"+ProvidedServiceStub.SERVICE_NAME+"/feed/entryId/15"); 
        this.control.expectAndDefaultReturn(this.request.getParameter("max-results"),null); 
        this.control.expectAndDefaultReturn(this.request.getParameter("alt"), 
                null); 
        this.control.expectAndDefaultReturn(this.request.getQueryString(), 
                null); 
        this.control.replay(); 
        this.feedRequest.initializeRequest(); 
        String nextId = "http://"+host+"/"+ProvidedServiceStub.SERVICE_NAME+"/feed"+"?max-results=25&start-index=26"; 
     
        assertEquals("next ID",nextId,this.feedRequest.getNextId()); 
        this.control.reset();

        
        this.control.expectAndDefaultReturn(this.request.getParameterMap(),this.parametermap);
        this.control.expectAndDefaultReturn(this.request.getHeader("Host"),host); 
        this.control.expectAndDefaultReturn(this.request.getRequestURI(),"/"+ProvidedServiceStub.SERVICE_NAME+"/feed/"); 
        this.control.expectAndDefaultReturn(this.request.getPathInfo(),"/"+ProvidedServiceStub.SERVICE_NAME+"/feed/entryId/15"); 
        this.control.expectAndReturn(this.request.getParameter("max-results"),null,3); 
        this.control.expectAndReturn(this.request.getParameter("start-index"),null,3);
        this.control.expectAndDefaultReturn(this.request.getParameter("alt"), 
                null); 
        this.control.expectAndDefaultReturn(this.request.getQueryString(), 
                null); 
        this.control.replay(); 
        this.feedRequest.initializeRequest(); 
        nextId = "http://"+host+"/"+ProvidedServiceStub.SERVICE_NAME+"/feed"+"?max-results=25&start-index=26"; 
        assertEquals("nextID",nextId,this.feedRequest.getNextId());
    } 
    
    public void testGetpreviousId() throws GDataRequestException{ 
        String host = "www.apache.org"; 
        String feedAndEntryID = "/"+ProvidedServiceStub.SERVICE_NAME+"/feed/entryid"; 
        String queryString = "max-results=25"; 
         
        Enumeration enu = new StringTokenizer("max-results",",");
        
        this.control.expectAndDefaultReturn(this.request.getParameterMap(),this.parametermap);
        this.control.expectAndDefaultReturn(this.request.getParameterNames(),enu);
        this.control.expectAndDefaultReturn(this.request.getHeader("Host"),host); 
        this.control.expectAndDefaultReturn(this.request.getRequestURI(),"/"+ProvidedServiceStub.SERVICE_NAME+"/feed/"); 
        this.control.expectAndDefaultReturn(this.request.getPathInfo(),"/"+ProvidedServiceStub.SERVICE_NAME+"/feed/"); 
        this.control.expectAndReturn(this.request.getParameter("start-index"),null); 
        this.control.expectAndDefaultReturn(this.request.getParameter("alt"), 
                null); 
        this.control.expectAndDefaultReturn(this.request.getQueryString(), 
                queryString); 
        this.control.replay(); 

        this.feedRequest.initializeRequest(); 
         
     
        assertNull(this.feedRequest.getPreviousId());
        this.control.verify();
        this.control.reset(); 
        String startIndex = "start-index="; 
        enu = new StringTokenizer("alt,max-results,start-index",",");
        queryString = "alt=rss&max-results=25&start-index=26"; 
        this.control.expectAndDefaultReturn(this.request.getParameterMap(),this.parametermap);
        this.control.expectAndDefaultReturn(this.request.getHeader("Host"),host); 
        this.control.expectAndDefaultReturn(this.request.getRequestURI(),"/"+ProvidedServiceStub.SERVICE_NAME+"/feed/"); 
        this.control.expectAndDefaultReturn(this.request.getPathInfo(),"/"+ProvidedServiceStub.SERVICE_NAME+"/feed/"); 
        this.control.expectAndReturn(this.request.getParameter("max-results"),"25",4); 
        this.control.expectAndReturn(this.request.getParameter("start-index"),"26",4); 
        this.control.expectAndDefaultReturn(this.request.getParameter("alt"), 
                "rss"); 
        this.control.expectAndDefaultReturn(this.request.getQueryString(), 
                queryString); 
        
        this.control.expectAndDefaultReturn(this.request.getParameterNames(),enu);
        
        
        this.control.replay(); 
        this.feedRequest.initializeRequest(); 
        
        String prevId = "http://"+host+"/"+ProvidedServiceStub.SERVICE_NAME+"/feed"+"?alt=rss&max-results=25&start-index=1"; 
        assertEquals("prevID",prevId,this.feedRequest.getPreviousId());
        
        this.control.reset(); 
         
        queryString = ""; 
        this.control.expectAndDefaultReturn(this.request.getParameterMap(),this.parametermap);
        this.control.expectAndDefaultReturn(this.request.getHeader("Host"),host); 
        this.control.expectAndDefaultReturn(this.request.getRequestURI(),"/"+ProvidedServiceStub.SERVICE_NAME+"/feed/"); 
        this.control.expectAndDefaultReturn(this.request.getPathInfo(),"/"+ProvidedServiceStub.SERVICE_NAME+"/feed/entryId/15"); 
        this.control.expectAndDefaultReturn(this.request.getParameter("max-results"),null); 
        this.control.expectAndDefaultReturn(this.request.getParameter("alt"), 
                null); 
        this.control.expectAndDefaultReturn(this.request.getQueryString(), 
                null); 
        this.control.replay(); 
        this.feedRequest.initializeRequest(); 
        assertNull(this.feedRequest.getPreviousId());
     
        
        this.control.reset(); 
        
        this.control.expectAndDefaultReturn(this.request.getParameterMap(),this.parametermap);
        this.control.expectAndDefaultReturn(this.request.getHeader("Host"),host); 
        this.control.expectAndDefaultReturn(this.request.getRequestURI(),"/"+ProvidedServiceStub.SERVICE_NAME+"/feed/"); 
        this.control.expectAndDefaultReturn(this.request.getPathInfo(),"/"+ProvidedServiceStub.SERVICE_NAME+"/feed/entryId/15"); 
        this.control.expectAndReturn(this.request.getParameter("max-results"),"35",3); 
        this.control.expectAndReturn(this.request.getParameter("start-index"),"5",3);
        this.control.expectAndDefaultReturn(this.request.getParameter("alt"), 
                null); 
        this.control.expectAndDefaultReturn(this.request.getQueryString(), 
                "max-results=35&start-index=5"); 
        this.control.replay(); 
        this.feedRequest.initializeRequest(); 
        prevId = "http://"+host+"/"+ProvidedServiceStub.SERVICE_NAME+"/feed"+"?max-results=35&start-index=1"; 
        assertEquals("prevID",prevId,this.feedRequest.getPreviousId());
        
        
        
        
    } 
    
    public void testGetContextPath(){
        String host = "www.apache.org"; 
        this.control.expectAndDefaultReturn(this.request.getHeader("Host"),host); 
        this.control.expectAndDefaultReturn(this.request.getRequestURI(),"/"+ProvidedServiceStub.SERVICE_NAME+"/feed/id/"); 
        this.control.expectAndDefaultReturn(this.request.getPathInfo(),"/"+ProvidedServiceStub.SERVICE_NAME+"/feed/id/"); 
        this.control.replay();
        String result = "http://"+host+"/"+ProvidedServiceStub.SERVICE_NAME+"/feed/id/";
        assertEquals(result,this.feedRequest.getContextPath());
        this.control.verify();
        this.control.reset();
        this.control.expectAndDefaultReturn(this.request.getHeader("Host"),host); 
        this.control.expectAndDefaultReturn(this.request.getRequestURI(),"/"+ProvidedServiceStub.SERVICE_NAME+"/feed/id"); 
        this.control.expectAndDefaultReturn(this.request.getPathInfo(),"/"+ProvidedServiceStub.SERVICE_NAME+"/feed/id"); 
        this.control.replay();
        
        assertEquals(result,this.feedRequest.getContextPath());
        this.control.verify();
        
    }
    
    public void testCategoryQuery() throws GDataRequestException{
        String host = "www.apache.org"; 
        String feedAndEntryID = "/feed"; 
        this.control.expectAndDefaultReturn(this.request.getParameterMap(),this.parametermap);
        this.control.expectAndDefaultReturn(this.request.getHeader("Host"),host); 
        this.control.expectAndDefaultReturn(this.request.getRequestURI(),"/host"+"/"+ProvidedServiceStub.SERVICE_NAME+"/feed/-/test"); 
        this.control.expectAndDefaultReturn(this.request.getPathInfo(),"/"+ProvidedServiceStub.SERVICE_NAME+"/feed/-/test"); 
         
        this.control.expectAndDefaultReturn(this.request.getParameter("alt"), 
                null); 
        this.control.expectAndDefaultReturn(this.request.getQueryString(), 
                null); 
        this.control.replay(); 
        this.feedRequest.initializeRequest(); 
         
     
        assertTrue(this.feedRequest.isFeedRequested()); 
        assertFalse(this.feedRequest.isEntryRequested()); 
        assertNotNull(this.feedRequest.getTranslatedQuery());
        this.control.verify();
        this.control.reset();
        this.feedRequest = new GDataRequest(this.request,GDataRequestType.GET); 
        this.control.expectAndDefaultReturn(this.request.getParameterMap(),this.parametermap);
        this.control.expectAndDefaultReturn(this.request.getHeader("Host"),host); 
        this.control.expectAndDefaultReturn(this.request.getRequestURI(),"/host"+"/"+ProvidedServiceStub.SERVICE_NAME+"/feed/"); 
        this.control.expectAndDefaultReturn(this.request.getPathInfo(),"/"+ProvidedServiceStub.SERVICE_NAME+"/feed/"); 
         
        this.control.expectAndDefaultReturn(this.request.getParameter("alt"), 
                null); 
        this.control.expectAndDefaultReturn(this.request.getQueryString(), 
                null); 
        this.control.replay(); 
        this.feedRequest.initializeRequest(); 
         
     
        assertTrue(this.feedRequest.isFeedRequested()); 
        assertFalse(this.feedRequest.isEntryRequested()); 
        assertNull(this.feedRequest.getTranslatedQuery());
        this.control.verify();
        this.control.reset(); 
    }
    
    
    
} 
