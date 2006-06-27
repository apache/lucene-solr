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
package org.apache.lucene.gdata.servlet.handler;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import junit.framework.TestCase;

import org.apache.lucene.gdata.data.GDataAccount;
import org.apache.lucene.gdata.data.ServerBaseFeed;
import org.apache.lucene.gdata.server.ServiceException;
import org.apache.lucene.gdata.server.ServiceFactory;
import org.apache.lucene.gdata.server.administration.AdminService;
import org.apache.lucene.gdata.server.registry.ComponentType;
import org.apache.lucene.gdata.server.registry.GDataServerRegistry;
import org.apache.lucene.gdata.server.registry.RegistryException;
import org.apache.lucene.gdata.servlet.handler.AbstractFeedHandler.FeedHandlerException;
import org.apache.lucene.gdata.utils.ProvidedServiceStub;
import org.apache.lucene.gdata.utils.ServiceFactoryStub;
import org.apache.lucene.gdata.utils.StorageStub;
import org.easymock.MockControl;

import com.google.gdata.util.ParseException;

/**
 * @author Simon Willnauer
 *
 */
public class TestAbstractFeedHandler extends TestCase {
    private MockControl requestMockControl; 
    
    private HttpServletRequest mockRequest = null; 
    
    private String accountName = "acc"; 
    private MockControl adminServiceMockControl;
    private AdminService adminService = null;
    private ServiceFactoryStub stub;
    private String serviceName = StorageStub.SERVICE_TYPE_RETURN;
    private static File incomingFeed = new File("src/test/org/apache/lucene/gdata/server/registry/TestEntityBuilderIncomingFeed.xml");
    BufferedReader reader;
    static{
        
        try {
            
            GDataServerRegistry.getRegistry().registerComponent(StorageStub.class);
            GDataServerRegistry.getRegistry().registerComponent(ServiceFactoryStub.class);
        } catch (RegistryException e) {
            
            e.printStackTrace();
        }
    }
    protected void setUp() throws Exception {
        super.setUp();
        
        GDataServerRegistry.getRegistry().registerService(new ProvidedServiceStub());
       this.requestMockControl = MockControl.createControl(HttpServletRequest.class);
       this.adminServiceMockControl = MockControl.createControl(AdminService.class);
       this.adminService = (AdminService)this.adminServiceMockControl.getMock();
       this.mockRequest = (HttpServletRequest)this.requestMockControl.getMock();
       this.stub = (ServiceFactoryStub)GDataServerRegistry.getRegistry().lookup(ServiceFactory.class,ComponentType.SERVICEFACTORY);
       this.stub.setAdminService(this.adminService);
       this.reader =  new BufferedReader(new FileReader(incomingFeed));
    }

    protected void tearDown() throws Exception {
        super.tearDown();
    }

    /*
     * Test method for 'org.apache.lucene.gdata.servlet.handler.AbstractFeedHandler.createFeedFromRequest(HttpServletRequest)'
     */
    public void testCreateFeedFromRequest() throws ParseException, IOException, FeedHandlerException {
        
        this.requestMockControl.expectAndDefaultReturn(this.mockRequest 
                .getParameter("service"), this.serviceName);
        this.requestMockControl.expectAndReturn(this.mockRequest.getReader(),this.reader);
        this.requestMockControl.replay();
        AbstractFeedHandler handler = new InsertFeedHandler();
        try{
        ServerBaseFeed feed = handler.createFeedFromRequest(this.mockRequest);
        assertNotNull(feed.getId());
        
        }catch (Exception e) {
            e.printStackTrace();
            fail("unexpected exception -- "+e.getMessage());
            
        }
        this.requestMockControl.verify();
        this.requestMockControl.reset();
        /*
         * Test for not registered service
         */
        this.requestMockControl.expectAndDefaultReturn(this.mockRequest 
                .getParameter("service"), "some other service");
        this.requestMockControl.replay();
         handler = new InsertFeedHandler();
        try{
        ServerBaseFeed feed = handler.createFeedFromRequest(this.mockRequest);
        
        fail(" exception expected");
        }catch (FeedHandlerException e) {
            e.printStackTrace();
            assertEquals(HttpServletResponse.SC_NOT_FOUND,handler.getErrorCode());
        }
        this.requestMockControl.verify();
        
        this.requestMockControl.reset();
        /*
         * Test for IOException
         */
        this.requestMockControl.expectAndDefaultReturn(this.mockRequest 
                .getParameter("service"), this.serviceName);
        this.reader.close();
        this.requestMockControl.expectAndReturn(this.mockRequest.getReader(),this.reader);
        this.requestMockControl.replay();
         handler = new InsertFeedHandler();
        try{
        ServerBaseFeed feed = handler.createFeedFromRequest(this.mockRequest);
        
        fail(" exception expected");
        }catch (IOException e) {
            e.printStackTrace();
            assertEquals(HttpServletResponse.SC_BAD_REQUEST,handler.getErrorCode());
        }
        this.requestMockControl.verify();
        
        
        
        
    }

    /*
     * Test method for 'org.apache.lucene.gdata.servlet.handler.AbstractFeedHandler.createRequestedAccount(HttpServletRequest)'
     */
    public void testCreateRequestedAccount() throws IOException, ParseException, ServiceException {
        this.requestMockControl.expectAndDefaultReturn(this.mockRequest 
                .getParameter(AbstractFeedHandler.PARAMETER_ACCOUNT), this.accountName);
        GDataAccount a = new GDataAccount();
        a.setName("helloworld");
        this.adminServiceMockControl.expectAndReturn(this.adminService.getAccount(this.accountName),a );
        this.requestMockControl.replay();
        this.adminServiceMockControl.replay();
        AbstractFeedHandler handler = new InsertFeedHandler();
        try{
            
            GDataAccount account = handler.createRequestedAccount(this.mockRequest);
       
        assertEquals(a,account);
        
        }catch (Exception e) {
            e.printStackTrace();
            fail("unexpected exception -- "+e.getMessage());
            
        }
        this.requestMockControl.verify();
        this.requestMockControl.reset();
        this.adminServiceMockControl.verify();
        this.adminServiceMockControl.reset();
        
        /*
         *Test for service exception 
         */
        
        this.requestMockControl.expectAndDefaultReturn(this.mockRequest 
                .getParameter(AbstractFeedHandler.PARAMETER_ACCOUNT), this.accountName);
        
        a.setName("helloworld");
        this.adminServiceMockControl.expectAndDefaultThrow(this.adminService.getAccount(this.accountName),new ServiceException() );
        this.requestMockControl.replay();
        this.adminServiceMockControl.replay();
         handler = new InsertFeedHandler();
        try{
            
            GDataAccount account = handler.createRequestedAccount(this.mockRequest);
       
            fail(" exception expected ");
        
        }catch (Exception e) {
            e.printStackTrace();
            assertEquals(HttpServletResponse.SC_BAD_REQUEST,handler.getErrorCode());
            
        }
        this.requestMockControl.verify();
        this.requestMockControl.reset();
        this.adminServiceMockControl.verify();
        this.adminServiceMockControl.reset();
        
        
        
        
        
    }

}
