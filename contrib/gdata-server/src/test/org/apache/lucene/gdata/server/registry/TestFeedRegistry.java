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
 
import com.google.gdata.data.Feed; 
 
import junit.framework.TestCase; 
 
/** 
 * @author Simon Willnauer 
 * 
 */ 
public class TestFeedRegistry extends TestCase { 
    private GDataServerRegistry reg; 
    private FeedInstanceConfigurator configurator; 
    @Override 
    protected void setUp(){ 
        this.reg = GDataServerRegistry.getRegistry(); 
        this.configurator = new FeedInstanceConfigurator(); 
    } 
    /** 
     * @see junit.framework.TestCase#tearDown() 
     */ 
    @Override 
    protected void tearDown() throws Exception { 
        this.reg.flushRegistry(); 
    } 
    /** 
     * Test method for 'org.apache.lucene.gdata.server.registry.FeedRegistry.getRegistry()' 
     */ 
    public void testGetRegistry() { 
         
        GDataServerRegistry reg1 = GDataServerRegistry.getRegistry(); 
        assertEquals("test singleton",this.reg,reg1); 
    } 
 
    /** 
     * Test method for 'org.apache.lucene.gdata.server.registry.FeedRegistry.registerFeed(FeedInstanceConfigurator)' 
     */ 
    public void testRegisterFeed() { 
        String feedURL = "myFeed"; 
        registerFeed(feedURL); 
        assertEquals("Registered Configurator",this.configurator,this.reg.getFeedConfigurator(feedURL)); 
        assertNull("not registered Configurator",this.reg.getFeedConfigurator("somethingElse")); 
        try{ 
            this.reg.getFeedConfigurator(null); 
            fail("Exception expected"); 
        }catch (IllegalArgumentException e) { 
            // 
        } 
    } 
 
    /** 
     * Test method for 'org.apache.lucene.gdata.server.registry.FeedRegistry.getFeedConfigurator(String)' 
     */ 
    public void testFlushRegistry() { 
        String feedURL = "testFeed"; 
        registerFeed(feedURL); 
        assertEquals("Registered Configurator",this.configurator,this.reg.getFeedConfigurator(feedURL)); 
        this.reg.flushRegistry(); 
        assertNull("Registry flushed",this.reg.getFeedConfigurator(feedURL)); 
         
 
    } 
     
    /** 
     *  
     */ 
    public void testIsFeedRegistered(){ 
        String myFeed = "myFeed"; 
        registerFeed(myFeed); 
        assertTrue("Feed is registerd",this.reg.isFeedRegistered(myFeed)); 
        assertFalse("null Feed is not registerd",this.reg.isFeedRegistered(null)); 
        assertFalse("Feed is not registerd",this.reg.isFeedRegistered("someOtherFeed")); 
         
    } 
     
    private void registerFeed(String feedURL){ 
         
        this.configurator.setFeedType(Feed.class); 
        this.configurator.setFeedId(feedURL); 
        this.reg.registerFeed(this.configurator); 
    } 
 
} 
