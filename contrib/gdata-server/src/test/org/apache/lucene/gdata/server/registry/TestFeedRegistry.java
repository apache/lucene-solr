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

import org.apache.lucene.gdata.server.ServiceFactory;
import org.apache.lucene.gdata.servlet.handler.DefaultRequestHandlerFactory;
import org.apache.lucene.gdata.servlet.handler.RequestHandlerFactory;
import org.apache.lucene.gdata.storage.StorageController;
import org.apache.lucene.gdata.storage.lucenestorage.StorageCoreController;

import com.google.gdata.data.Entry;
import com.google.gdata.data.Feed;

import junit.framework.TestCase;

/**
 * @author Simon Willnauer
 *
 */
public class TestFeedRegistry extends TestCase {
	private GDataServerRegistry reg;
	private ProvidedServiceConfig configurator;
	@Override
	protected void setUp(){
		this.reg = GDataServerRegistry.getRegistry();
		this.configurator = new ProvidedServiceConfig();
        this.configurator.setEntryType(Entry.class);
        this.configurator.setFeedType(Feed.class);
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
	public void testRegisterService() {
		String service = "service";
		registerService(service);
		assertEquals("Registered Configurator",this.configurator,this.reg.getProvidedService(service));
		assertNull("not registered Configurator",this.reg.getProvidedService("something"));
		try{
			this.reg.getProvidedService(null);
			fail("Exception expected");
		}catch (IllegalArgumentException e) {
			//
		}
	}

	/**
	 * Test method for 'org.apache.lucene.gdata.server.registry.FeedRegistry.getFeedConfigurator(String)'
	 */
	public void testFlushRegistry() {
        String service = "service";
        registerService(service);
		
		assertEquals("Registered Configurator",this.configurator,this.reg.getProvidedService(service));
		this.reg.flushRegistry();
		assertNull("Registry flushed",this.reg.getProvidedService(service));
		

	}
	
	/**
	 * 
	 */
	public void testIsFeedRegistered(){
        String service = "service";
        registerService(service);
		assertTrue("Feed is registerd",this.reg.isServiceRegistered(service));
		assertFalse("null Feed is not registerd",this.reg.isServiceRegistered(null));
		assertFalse("Feed is not registerd",this.reg.isServiceRegistered("something"));
		
	}
	
	private void registerService(String servicename){
		
		this.configurator.setName(servicename);
		this.reg.registerService(this.configurator);
	}
    
    public void testRegisterComponent() throws RegistryException{
        try {
            this.reg.registerComponent(StorageController.class);
            fail("RegistryException expected");
        } catch (RegistryException e) {
        //
        }
        this.reg.registerComponent(StorageCoreController.class);
        
        this.reg.registerComponent(DefaultRequestHandlerFactory.class);
        RequestHandlerFactory factory =  this.reg.lookup(RequestHandlerFactory.class,ComponentType.REQUESTHANDLERFACTORY);
        try{
        this.reg.registerComponent(DefaultRequestHandlerFactory.class);
        fail("RegistryException expected");
        } catch (RegistryException e) {
        //
        }
        this.reg.registerComponent(ServiceFactory.class);
        ServiceFactory servicefactory =  this.reg.lookup(ServiceFactory.class,ComponentType.SERVICEFACTORY);
        assertNotNull(servicefactory);
        assertNotNull(factory);
        
        
    }

}
