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
package org.apache.lucene.gdata.server.registry; 
 
import java.io.File; 
import java.io.FileReader; 
import java.io.IOException; 
import java.io.Reader; 
 
import junit.framework.TestCase; 
 
import org.apache.lucene.gdata.server.FeedNotFoundException; 
import org.apache.lucene.gdata.server.GDataEntityBuilder; 
 
import com.google.gdata.data.BaseEntry; 
import com.google.gdata.data.BaseFeed; 
import com.google.gdata.data.Entry; 
import com.google.gdata.data.ExtensionProfile; 
import com.google.gdata.data.Feed; 
import com.google.gdata.util.ParseException; 
 
/** 
 * @author Simon Willnauer 
 * 
 */ 
public class TestGDataEntityBuilder extends TestCase { 
    private static File incomingFeed = new File("src/test/org/apache/lucene/gdata/server/registry/TestEntityBuilderIncomingFeed.xml"); 
    private static File incomingEntry = new File("src/test/org/apache/lucene/gdata/server/registry/TestEntityBuilderIncomingEntry.xml"); 
    private static String feedTitleFromXML = "Simon Willnauer"; 
    private static String entrySummaryFromXML = "When: 2006-12-23 to 2006-12-31 America/Los_Angeles"; 
    private static GDataServerRegistry reg = GDataServerRegistry.getRegistry(); 
    private Reader reader;  
    private static String feedID = "myFeed"; 
    private ExtensionProfile profile; 
    private static Class feedType = Feed.class; 
     
     
    /** 
     * @see junit.framework.TestCase#setUp() 
     */ 
    @Override 
    protected void setUp() throws Exception { 
        FeedInstanceConfigurator config = new FeedInstanceConfigurator(); 
        config.setFeedId(feedID); 
        config.setFeedType(feedType); 
        this.profile = new ExtensionProfile(); 
        reg.registerFeed(config); 
    } 
 
    /** 
     * @see junit.framework.TestCase#tearDown() 
     */ 
    @Override 
    protected void tearDown() throws Exception { 
        reg.flushRegistry(); 
        this.reader = null; 
    } 
 
    /** 
     * Test method for 'org.apache.lucene.gdata.data.GDataEntityBuilder.buildFeed(String, Reader)' 
     */ 
    public void testBuildFeedStringReader() throws FeedNotFoundException, ParseException, IOException { 
        this.reader = new FileReader(incomingFeed); 
        BaseFeed feed = GDataEntityBuilder.buildFeed(feedID,this.reader,this.profile); 
        assertNotNull(feed); 
        assertEquals("feed title",feed.getTitle().getPlainText(), feedTitleFromXML); 
        assertTrue( feed instanceof Feed); 
         
    } 
 
    /* 
     * Test method for 'org.apache.lucene.gdata.data.GDataEntityBuilder.buildEntry(String, Reader)' 
     */ 
    public void testBuildEntryStringReader() throws FeedNotFoundException, ParseException, IOException { 
        this.reader = new FileReader(incomingEntry); 
        BaseEntry entry = GDataEntityBuilder.buildEntry(feedID,this.reader,this.profile); 
        assertNotNull(entry); 
        assertEquals("entry summary",entry.getSummary().getPlainText(),entrySummaryFromXML); 
        assertTrue(entry instanceof Entry); 
         
    } 
     
     
 
} 
