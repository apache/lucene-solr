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
package org.apache.lucene.gdata.hivemind.webservice;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import junit.framework.TestCase;

import org.easymock.MockControl;

import com.caucho.hessian.server.HessianSkeleton;

/**
 * @author Simon Willnauer
 * 
 */
public class HessianSkeletonProviderImplTest extends TestCase {
    HessianSkeletonProviderImpl provider;

    MockControl<HttpServletRequest> mockControl;

    HttpServletRequest mockedRequest;

    static String mapKey = "test";

    static String testPathSuccess = "/endpoint/" + mapKey;

    static String testPathFail = "/endpoint/fail";

    private Map<String, WebserviceMappingBean> mapping;

    protected void setUp() throws Exception {
        this.mockControl = MockControl.createControl(HttpServletRequest.class);
        this.mockedRequest = this.mockControl.getMock();
        this.provider = new HessianSkeletonProviderImpl();
        this.mapping = new HashMap<String, WebserviceMappingBean>();
        WebserviceMappingBean bean = new WebserviceMappingBean();

        bean.setServiceImpl(new TestService());
        bean.setServiceInterface(Serializable.class);
        this.mapping.put(mapKey, bean);
        this.provider.setMapping(this.mapping);

    }

    protected void tearDown() throws Exception {
        super.tearDown();
    }

    /*
     * Test method for
     * 'org.apache.lucene.gdata.hivemind.webservice.HessianSkeletonProviderImpl.getServiceSkeletonInvoker(HttpServletRequest)'
     */
    public void testGetServiceSkeletonInvoker() {
        this.mockControl.expectAndDefaultReturn(this.mockedRequest
                .getPathInfo(), testPathSuccess);
        this.mockControl.replay();
        assertNotNull(this.provider
                .getServiceSkeletonInvoker(this.mockedRequest));
        this.mockControl.verify();
        this.mockControl.reset();

        this.mockControl.expectAndDefaultReturn(this.mockedRequest
                .getPathInfo(), testPathFail);
        this.mockControl.replay();
        try {
            assertNotNull(this.provider
                    .getServiceSkeletonInvoker(this.mockedRequest));
            fail("Service should not be found");
        } catch (NoSuchServiceException e) {
            //
        }
        this.mockControl.verify();
        this.mockControl.reset();
    }

    /**
     * 
     */
    public void testGetMappingFromPath() {
        try {
            this.provider.getMappingFromPath(testPathFail);
            fail("Service should not be found");
        } catch (NoSuchServiceException e) {
            //
        }

        HessianSkeleton retVal = this.provider
                .getMappingFromPath(testPathSuccess);
        assertNotNull(retVal);
        HessianSkeleton retVal1 = this.provider
                .getMappingFromPath(testPathSuccess + "/");
        assertEquals(retVal, retVal1);
        assertNotNull(retVal);
        assertEquals(Serializable.class.getName(), retVal.getAPIClassName());

    }

    /**
     * 
     */
    public void testVerifyInterfaceImpl() {
        this.mapping.get(mapKey).setServiceImpl(new WebserviceMappingBean());
        try {
            this.provider.getMappingFromPath(testPathSuccess);
            fail("Impl is not assignable to Interface");
        } catch (RuntimeException e) {
            //
        }
    }

    private static class TestService implements Serializable {
        // just for test case
    }

}
