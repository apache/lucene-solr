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

import junit.framework.TestCase;

import com.google.gdata.data.Entry;
import com.google.gdata.data.ExtensionProfile;
import com.google.gdata.data.Feed;

public class TestProvidedServiceConfig extends TestCase {
    ProvidedServiceConfig instance;
    protected void setUp() throws Exception {
        instance = new ProvidedServiceConfig();
        instance.setExtensionProfileClass(ExtensionProfile.class);
        instance.setFeedType(Feed.class);
        instance.setEntryType(Entry.class);
    }

    protected void tearDown() throws Exception {
        super.tearDown();
        instance.destroy();
    }

    /*
     * Test method for 'org.apache.lucene.gdata.server.registry.ProvidedServiceConfig.getExtensionProfile()'
     */
    public void testGetExtensionProfile() {
        try{
        this.instance.setExtensionProfile(null);
        fail("value must not be null");
        }catch (IllegalArgumentException e) {

        }
        ExtensionProfile profile = this.instance.getExtensionProfile();
        assertNotNull(profile);
        assertSame(profile,this.instance.getExtensionProfile());
        this.instance.visiteInitialize();
        assertSame(profile,this.instance.getExtensionProfile());
        assertNull(new ProvidedServiceConfig().getExtensionProfile());
    }

    /*
     * Test method for 'org.apache.lucene.gdata.server.registry.ProvidedServiceConfig.setExtensionProfile(ExtensionProfile)'
     */
    public void testSetPoolSize() throws InstantiationException, IllegalAccessException {
        assertEquals(5,instance.getPoolSize());
        instance.destroy();
        instance = new ProvidedServiceConfig();
        instance.setExtensionProfileClass(ExtensionProfile.class);
        instance.setFeedType(Feed.class);
        instance.setEntryType(Entry.class);
        instance.setPoolSize(30);
        instance.visiteInitialize();
        assertEquals(30,instance.getPoolSize());
        instance.destroy();
        instance = new ProvidedServiceConfig();
        instance.setExtensionProfileClass(ExtensionProfile.class);
        instance.setFeedType(Feed.class);
        instance.setEntryType(Entry.class);
        instance.setPoolSize(-5);
        instance.visiteInitialize();
        assertEquals(5,instance.getPoolSize());
    }

   
   
   
    /*
     * Test method for 'org.apache.lucene.gdata.server.registry.ProvidedServiceConfig.visiteInitialize()'
     */
    public void testVisiteInitialize() {
        instance.visiteInitialize();
        assertNull(instance.extProfThreadLocal.get());
        instance.getExtensionProfile();
        assertNotNull(instance.extProfThreadLocal.get());
    }

    /*
     * Test method for 'org.apache.lucene.gdata.server.registry.ProvidedServiceConfig.visiteDestroy()'
     */
    public void testVisiteDestroy() {
        ExtensionProfile profile = this.instance.getExtensionProfile();
        assertNotNull(profile);
        assertNotNull(instance.extProfThreadLocal.get());
        instance.visiteDestroy();
        assertNull(instance.extProfThreadLocal.get());
        instance.visiteDestroy();
        assertNull(instance.extProfThreadLocal.get());
    }

}
