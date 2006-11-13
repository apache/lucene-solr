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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.servlet.http.HttpServletRequest;

import com.caucho.hessian.server.HessianSkeleton;

/**
 * Simple provider implementation.
 * @author Simon Willnauer
 * 
 */
public class HessianSkeletonProviderImpl implements HessianSkeletonProvider {
    private Map<String, WebserviceMappingBean> mapping;

    private ConcurrentHashMap<String, HessianSkeleton> skeletonCache = new ConcurrentHashMap<String, HessianSkeleton>();

    /**
     * Creates a new HessianSkeletonProviderImpl instance
     */
    public HessianSkeletonProviderImpl() {
        super();

    }

    /**
     * The last part of the request URL is used to identify a configured service
     * mapping.
     * 
     * @param path -
     *            the servletrequest path info
     * @return - the corresponding HessianSkeleton
     */
    protected HessianSkeleton getMappingFromPath(String path) {
        if (path.endsWith("/"))
            path = path.substring(0, path.length() - 1);
        String requestedService = path.substring(path.lastIndexOf("/") + 1);
        HessianSkeleton retVal = this.skeletonCache.get(requestedService);
        if (retVal == null) {
            WebserviceMappingBean wsBean = this.mapping.get(requestedService);
            if (wsBean == null)
                throw new NoSuchServiceException();
            if (!checkInterface(wsBean))
                throw new RuntimeException(
                        "The configured webservice interface is not assignable from the corresponding service");
            retVal = new HessianSkeleton(wsBean.getServiceImpl(), wsBean
                    .getServiceInterface());
            /*
             * rather create this service twice as synchronize the whole block
             */
            this.skeletonCache.putIfAbsent(requestedService, retVal);
        }
        return retVal;
    }

    @SuppressWarnings("unchecked")
    private boolean checkInterface(WebserviceMappingBean bean) {
        return bean.getServiceInterface().isAssignableFrom(
                bean.getServiceImpl().getClass());
    }

    /**
     * @see org.apache.lucene.gdata.hivemind.webservice.HessianSkeletonProvider#getServiceSkeletonInvoker(javax.servlet.http.HttpServletRequest)
     */
    public HessianServiceSkeletonInvoker getServiceSkeletonInvoker(
            HttpServletRequest arg0) {
        if (arg0 == null)
            throw new IllegalArgumentException(
                    "HttpServletRequest must not be null");
        HessianSkeleton mappingFromRequest = getMappingFromPath(arg0
                .getPathInfo());
        return new HessianServiceSkeletonInvokerImpl(mappingFromRequest);
    }

    /**
     * @return Returns the mapping.
     */
    public Map<String, WebserviceMappingBean> getMapping() {
        return this.mapping;
    }

    /**
     * @param mapping
     *            The mapping to set.
     */
    public void setMapping(Map<String, WebserviceMappingBean> mapping) {
        this.mapping = mapping;
    }

}
